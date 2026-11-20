// 데모 CLI 의 화면 문구. 문구는 en.json / ko.json 두 파일에만 둔다(코드에 문구를 흩뿌리지 않는다).
// 언어 결정 순서: --lang > MCP_STREAMSTORE_LANG > LC_ALL > LC_MESSAGES > LANG. 한국어가 아니면 영어.
import en from './en.json' with { type: 'json' };
import ko from './ko.json' with { type: 'json' };

export type Lang = 'en' | 'ko';
export type MessageKey = keyof typeof en;

export const MESSAGES: Record<Lang, Record<string, string>> = { en, ko };

/** Pick the language from an explicit flag, then the environment. */
export function resolveLang(flag: string | undefined, env: NodeJS.ProcessEnv = process.env): Lang {
  if (flag !== undefined) return normalize(flag) ?? 'en';
  const candidates = [env.MCP_STREAMSTORE_LANG, env.LC_ALL, env.LC_MESSAGES, env.LANG];
  for (const value of candidates) {
    if (value && value.trim() !== '') return normalize(value) ?? 'en'; // 처음 설정된 값이 결정한다
  }
  return 'en';
}

/** "ko", "ko_KR.UTF-8", "ko-KR" → ko; "en", "en_US.UTF-8", "C" → en; anything else → undefined. */
export function normalize(value: string): Lang | undefined {
  const v = value.trim().toLowerCase();
  if (/^ko([_.\-@]|$)/.test(v)) return 'ko';
  if (v !== '') return 'en';
  return undefined;
}

/** True when `value` is exactly a supported --lang value. */
export function isLang(value: string): value is Lang {
  return value === 'en' || value === 'ko';
}

/** Translator bound to one language. `{name}` placeholders are filled from `vars`. */
export function translator(lang: Lang) {
  const table = MESSAGES[lang];
  return (key: MessageKey, vars: Record<string, string | number> = {}): string => {
    const template = table[key] ?? MESSAGES.en[key] ?? key;
    return template.replace(/\{(\w+)\}/g, (m, name: string) => (name in vars ? String(vars[name]) : m));
  };
}
