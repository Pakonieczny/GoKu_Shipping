"""Protocol checks with an explicit fake model; no neural quality claim."""
import importlib.util,json,multiprocessing,socket,time,unittest,urllib.request,urllib.error
from pathlib import Path
spec=importlib.util.spec_from_file_location('runner',Path(__file__).parents[2]/'tools/learned-nest/runner.py');r=importlib.util.module_from_spec(spec);spec.loader.exec_module(r)
class Fake:
 device='test';digest='test'
 def predict(self,request): return {'model':'TEST FIXTURE','candidates':[]}
class Protocol(unittest.TestCase):
 @classmethod
 def setUpClass(cls):
  with socket.socket() as s:s.bind(('127.0.0.1',0));cls.port=s.getsockname()[1]
  cls.proc=multiprocessing.Process(target=r.serve,args=(Fake(),'test-secret',cls.port,{'https://goldenspike.app'}),daemon=True);cls.proc.start()
  for _ in range(100):
   try:
    with socket.create_connection(('127.0.0.1',cls.port),timeout=.1):break
   except OSError:time.sleep(.01)
 @classmethod
 def tearDownClass(cls):cls.proc.terminate();cls.proc.join()
 def request(self,path='/health',method='GET',origin='https://goldenspike.app',token='test-secret',data=None):
  req=urllib.request.Request(f'http://127.0.0.1:{self.port}'+path,method=method,headers={'Origin':origin,'Authorization':'Bearer '+token,'Content-Type':'application/json'},data=data)
  try:return urllib.request.urlopen(req,timeout=3)
  except urllib.error.HTTPError as e:return e
 def test_health(self):
  response=self.request();self.assertEqual(response.status,200);self.assertTrue(json.load(response)['ready']);self.assertEqual(response.headers['Cache-Control'],'no-store')
 def test_security(self):
  self.assertEqual(self.request(origin='https://attacker.example').status,403)
  self.assertEqual(self.request(token='wrong').status,403)
  self.assertEqual(self.request(origin='null').status,403)
 def test_preflight(self):
  response=self.request(method='OPTIONS');self.assertEqual(response.status,204);self.assertEqual(response.headers['Access-Control-Allow-Private-Network'],'true')
 def test_body(self):
  self.assertEqual(self.request('/propose','POST',data=b'{').status,400)
  self.assertEqual(self.request('/propose','POST',data=b'{}').status,200)
  self.assertEqual(self.request('/bad','POST',data=b'{}').status,404)
if __name__=='__main__':unittest.main()
