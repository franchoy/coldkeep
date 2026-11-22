import { chromium } from 'playwright';
import { readFileSync, writeFileSync, statSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { join } from 'node:path';
const [configPath, reportPath] = process.argv.slice(2);
const config=JSON.parse(readFileSync(configPath,'utf8'));
const browser=await chromium.launch({headless:true});
const context=await browser.newContext({serviceWorkers:'block'});
const external=[];const loaded=[];
await context.route('**/*',async route=>{
 const url=new URL(route.request().url());
 if(url.origin!==config.public_url){external.push(url.origin);await route.abort();}
 else {loaded.push(url.pathname);await route.continue();}
});
try {
 const page=await context.newPage();
 const errors=[];page.on('pageerror',e=>errors.push(e.message));
 await page.goto(config.public_url,{waitUntil:'networkidle'});
 if(!(await page.getByRole('heading',{name:'Internal delivery proof'}).isVisible()))throw Error('internal status absent');
 await page.getByLabel('One-use bootstrap secret').fill(readFileSync(join(config.runtime_dir,'bootstrap'),'utf8'));
 await page.getByRole('button',{name:'Authenticate',exact:true}).click();
 await page.getByRole('heading',{name:'Inherited fixture history'}).waitFor();
 const expected={
  'history/Case.txt':['8950abfda7b727630760dd35bcf5c3daa7631aff223a90f7728c0d2521dde10c',4],
  'history/empty.txt':['e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',0],
  'history/encrypted.bin':['6b1d4bfe25f0144686cade6a6f77f222e6ef929b949a9d688e418d56dd3af609',4096]
 };
 const outputs=[];
 for(const [name,[hash,size]] of Object.entries(expected)) {
  const radio=page.locator('label.entry').filter({hasText:name}).getByRole('radio');
  await radio.focus();await page.keyboard.press('Space');
  await page.getByRole('button',{name:'Restore selected file',exact:true}).click();
  await page.getByRole('status').filter({hasText:'completed:'}).waitFor();
  const raw=readFileSync(join(config.restore_root,name));const actual=createHash('sha256').update(raw).digest('hex');const st=statSync(join(config.restore_root,name));
  if(actual!==hash || raw.length!==size || (st.mode&0o777)!==0o640 || st.mtimeMs!==1577934245000)throw Error('fixed independent restore oracle mismatch: '+name);
  outputs.push({name,expected_sha256:hash,actual_sha256:actual,bytes:raw.length,mode:'0640',mtime:'2020-01-02T03:04:05Z'});
 }
 if(external.length || errors.length)throw Error('external traffic or browser errors');
 if(!loaded.some(p=>p.startsWith('/assets/')&&p.endsWith('.js')) || !loaded.includes('/api/v1/history') || !loaded.includes('/api/v1/restore'))throw Error('missing real browser/engine requests');
 await page.screenshot({path:reportPath+'.png',fullPage:true});
 writeFileSync(reportPath,JSON.stringify({event:'packaged_browser_history_restore',browser:browser.version(),external_blocking:true,external_requests:external,loaded_paths:[...new Set(loaded)],page_errors:errors,outputs,keyboard_selection:true,body:await page.locator('main').innerText()},null,2)+'\n');
} finally {await context.close();await browser.close();}
console.log('PROOF_EVENT packaged_browser_history_restore browser_closed');
