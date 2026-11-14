// Small proof UI policy lint in addition to strict TypeScript/unused checks.
import { readFileSync, readdirSync } from 'node:fs';
for (const name of readdirSync('src').filter(n => /\.tsx?$/.test(n))) {
 const source = readFileSync(`src/${name}`, 'utf8');
 if (/localStorage|sessionStorage|dangerouslySetInnerHTML|document\.cookie|https?:\/\//.test(source)) {
  throw new Error(`Persistent credentials, injected HTML, or remote dependency in ${name}`);
 }
}
