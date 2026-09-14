// REST resumable protocol: https://developers.google.com/google-ads/api/docs/assets/upload-videos
const {uploadUrl} = require('./googleAdsMotionPublication');
function createVideoUpload({fetch, headers, customerId, version, beforeRequest=async()=>{}, onResponse=async()=>{}}) {
  async function request(url, options) {
    await beforeRequest();
    const response = await fetch(url, {...options, redirect:'error', timeout:120000});
    const data = await response.json().catch(() => ({}));
    await onResponse(data,response);
    if (!response.ok) throw Error('Google video upload: '+(data.error?.message || 'HTTP '+response.status));
    return {response, data};
  }
  return {
    async startUpload({bytes, title, description}) {
      const {response} = await request('https://googleads.googleapis.com/resumable/upload/'+version+'/customers/'+customerId+'/youTubeVideoUploads:create', {
        method:'POST', headers:{...await headers(), 'X-Goog-Upload-Protocol':'resumable', 'X-Goog-Upload-Command':'start', 'X-Goog-Upload-Header-Content-Length':String(bytes)},
        body:JSON.stringify({customer_id:customerId, you_tube_video_upload:{video_title:title.slice(0,100), video_description:description.slice(0,5000), video_privacy:'UNLISTED'}})
      });
      return {url:uploadUrl(response.headers.get('x-goog-upload-url'))};
    },
    async queryUpload(url) {
      const {response, data} = await request(uploadUrl(url), {method:'POST', headers:{...await headers(), 'X-Goog-Upload-Command':'query'}});
      const offset = response.headers.get('x-goog-upload-size-received');
      return {offset:offset === null ? NaN : Number(offset), resourceName:data.resourceName};
    },
    async finishUpload(url, bytes, offset) {
      const {data} = await request(uploadUrl(url), {method:'PUT', headers:{...await headers(), 'Content-Type':'application/octet-stream', 'Content-Length':String(bytes.length), 'X-Goog-Upload-Offset':String(offset), 'X-Goog-Upload-Command':'upload, finalize'}, body:bytes});
      return data;
    }
  };
}
module.exports = {createVideoUpload};
