from fontTools.ttLib import TTFont
from fontTools.varLib.instancer import instantiateVariableFont
import uharfbuzz as hb, pathlib, json, hashlib, tempfile, urllib.request
root=pathlib.Path(__file__).resolve().parents[2]
p=pathlib.Path(tempfile.mkdtemp(prefix='engraving-font-')); out=root/'vendor/fonts'
sources={
 'NotoEmoji.ttf':('https://raw.githubusercontent.com/google/fonts/main/ofl/notoemoji/NotoEmoji%5Bwght%5D.ttf', 'de6c18832938afc99caf132b39d6a30a19bac7f2e812e28db2535b4608d27551'),
 'NotoEmoji-OFL.txt':('https://raw.githubusercontent.com/google/fonts/main/ofl/notoemoji/OFL.txt', '500bb1ccf43df7bbb522112f9133a52b16e1c35e809632f5d8609b179152de5b'),
 'emoji-test.txt':('https://www.unicode.org/Public/17.0.0/emoji/emoji-test.txt', '1d8a944f88d7952f7ef7c5167fef3c67995bcae24543949710231b03a201acda')
}
for name,(url,expected) in sources.items():
 data=urllib.request.urlopen(url,timeout=60).read()
 if hashlib.sha256(data).hexdigest()!=expected: raise RuntimeError(name+': upstream changed; review before rebuilding')
 (p/name).write_bytes(data)
f=instantiateVariableFont(TTFont(p/'NotoEmoji.ttf',recalcTimestamp=False),{'wght':400},inplace=True)
f.save(out/'NotoEmoji-Regular.ttf')
(out/'NotoEmoji-OFL.txt').write_bytes((p/'NotoEmoji-OFL.txt').read_bytes())
data=(out/'NotoEmoji-Regular.ttf').read_bytes(); hf=hb.Font(hb.Face(data)); hb.ot_font_set_funcs(hf)
sequences={}; missing=[]
for line in (p/'emoji-test.txt').read_text().splitlines():
 if not line or line.startswith('#'): continue
 codes,status=line.split('#')[0].split(';'); text=''.join(chr(int(c,16)) for c in codes.split()); buf=hb.Buffer();buf.add_str(text);buf.guess_segment_properties();hb.shape(hf,buf)
 infos=buf.glyph_infos; poses=buf.glyph_positions
 if any(i.codepoint==0 for i in infos): missing.append(text);continue
 sequences[text]=[[i.codepoint,v.x_advance,v.x_offset,v.y_offset] for i,v in zip(infos,poses)]
(out/'emoji-sequences.json').write_text(json.dumps({'fontSha256':hashlib.sha256(data).hexdigest(),'source':'Google Fonts Noto Emoji / Unicode emoji-test 17.0','unsupported':missing,'sequences':sequences},ensure_ascii=False,separators=(',',':')))
print('font bytes',len(data),'sequences',len(sequences),'unsupported',len(missing),'examples',repr(missing[:20]))
