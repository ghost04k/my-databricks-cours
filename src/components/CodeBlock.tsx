"use client";

import { useState } from "react";

interface CodeBlockProps {
  code: string;
  language: string;
  title?: string;
}

/* ───── Keyword lists ───── */

const SQL_KEYWORDS = [
  "SELECT","FROM","WHERE","INSERT","INTO","UPDATE","DELETE","CREATE","DROP",
  "ALTER","TABLE","DATABASE","VIEW","INDEX","JOIN","INNER","LEFT","RIGHT",
  "OUTER","ON","AND","OR","NOT","IN","EXISTS","BETWEEN","LIKE","IS","NULL",
  "AS","ORDER","BY","GROUP","HAVING","LIMIT","OFFSET","UNION","ALL",
  "DISTINCT","CASE","WHEN","THEN","ELSE","END","SET","VALUES","WITH",
  "TEMPORARY","TEMP","IF","REPLACE","USING","DELTA","LOCATION","OPTIONS",
  "COMMENT","DESCRIBE","SHOW","USE","SCHEMA","CATALOG","GRANT","REVOKE",
  "DENY","OVERWRITE","PARTITIONED","TBLPROPERTIES","MERGE","MATCHED",
  "FORMAT","EXCEPT","INTERSECT","LATERAL","EXPLODE","PIVOT","UNPIVOT",
  "CONSTRAINT","PRIMARY","KEY","REFERENCES","FOREIGN","CASCADE","RESTRICT",
  "ENABLE","STREAMING","LIVE","APPLY","CHANGES","SEQUENCE","KEYS","STORED",
  "IGNORE","NULLS","FIRST","LAST","ROWS","RANGE","UNBOUNDED","PRECEDING",
  "FOLLOWING","CURRENT","ROW","OVER","PARTITION","WINDOW",
];

const SQL_BUILTINS = [
  "COUNT","SUM","AVG","MIN","MAX","COALESCE","CAST","CONCAT","SUBSTRING",
  "TRIM","UPPER","LOWER","LENGTH","REPLACE","SPLIT","DATE_FORMAT",
  "CURRENT_DATE","CURRENT_TIMESTAMP","DATEDIFF","DATE_ADD","DATE_SUB",
  "YEAR","MONTH","DAY","HOUR","MINUTE","SECOND","ROUND","FLOOR","CEIL",
  "ABS","ARRAY","MAP","STRUCT","NAMED_STRUCT","COLLECT_LIST","COLLECT_SET",
  "ROW_NUMBER","RANK","DENSE_RANK","LAG","LEAD","FIRST_VALUE","LAST_VALUE",
  "NTH_VALUE","NTILE","PERCENT_RANK","CUME_DIST","TO_DATE","TO_TIMESTAMP",
  "FROM_JSON","TO_JSON","SCHEMA_OF_JSON","FLATTEN","SIZE","ELEMENT_AT",
  "FILTER","TRANSFORM","REDUCE","AGGREGATE","EXISTS","ANY","EVERY",
  "REGEXP_EXTRACT","REGEXP_REPLACE","TRANSLATE","INITCAP","LPAD","RPAD",
  "SHA2","MD5","BASE64","UNBASE64","HEX","UNHEX","CRC32",
];

const PYTHON_KEYWORDS = [
  "def","class","import","from","return","if","elif","else","for","while",
  "try","except","finally","with","as","yield","lambda","pass","break",
  "continue","raise","in","not","and","or","is","del","global","nonlocal",
  "assert","async","await",
];

const PYTHON_CONSTANTS = ["None","True","False"];

const PYTHON_BUILTINS = [
  "print","len","range","type","int","str","float","bool","list","dict",
  "set","tuple","map","filter","zip","enumerate","sorted","reversed",
  "isinstance","issubclass","hasattr","getattr","setattr","super","property",
  "staticmethod","classmethod","abs","min","max","sum","round","open",
  "input","format","id","hex","oct","bin","chr","ord","repr",
  "display","spark","dbutils","df",
];

/* ───── Language icons ───── */

function LangIcon({ lang }: { lang: string }) {
  const l = lang.toLowerCase();
  if (l === "python") {
    return (
      <svg viewBox="0 0 24 24" className="w-4 h-4" fill="none">
        <path d="M12 2C6.48 2 6 4.02 6 5.5V8h6v1H5.5C3.02 9 1 10.52 1 13.5S3.02 18 5.5 18H8v-2.5C8 13.02 10.02 11 12.5 11H17c1.1 0 2-.9 2-2V5.5C19 3.02 17.02 2 14.5 2H12zm-1.5 2a1 1 0 110 2 1 1 0 010-2z" fill="#3572A5"/>
        <path d="M12 22c5.52 0 6-2.02 6-3.5V16h-6v-1h6.5c2.48 0 4.5-1.52 4.5-4.5S20.98 6 18.5 6H16v2.5c0 2.48-2.02 4.5-4.5 4.5H7c-1.1 0-2 .9-2 2v3.5C5 20.98 6.98 22 9.5 22H12zm1.5-2a1 1 0 110-2 1 1 0 010 2z" fill="#FFD43B"/>
      </svg>
    );
  }
  if (l === "sql") {
    return (
      <svg viewBox="0 0 24 24" className="w-4 h-4" fill="none">
        <rect x="3" y="3" width="18" height="18" rx="3" fill="#336791" opacity="0.15"/>
        <text x="12" y="16" textAnchor="middle" fontSize="9" fontWeight="bold" fill="#336791" fontFamily="monospace">SQL</text>
      </svg>
    );
  }
  return (
    <svg viewBox="0 0 24 24" className="w-4 h-4" fill="none" stroke="currentColor" strokeWidth="1.5">
      <path strokeLinecap="round" strokeLinejoin="round" d="M17.25 6.75L22.5 12l-5.25 5.25m-10.5 0L1.5 12l5.25-5.25m7.5-3l-4.5 16.5"/>
    </svg>
  );
}

/* ───── Highlight engine ───── */

type Token = { type: string; value: string };

function tokenize(code: string, language: string): Token[] {
  const lang = language.toLowerCase();
  const tokens: Token[] = [];
  let i = 0;

  while (i < code.length) {
    // Line comment
    if (lang === "python" && code[i] === "#") {
      let end = code.indexOf("\n", i);
      if (end === -1) end = code.length;
      tokens.push({ type: "comment", value: code.slice(i, end) });
      i = end;
      continue;
    }
    if (lang === "sql" && code[i] === "-" && code[i + 1] === "-") {
      let end = code.indexOf("\n", i);
      if (end === -1) end = code.length;
      tokens.push({ type: "comment", value: code.slice(i, end) });
      i = end;
      continue;
    }

    // Triple-quoted strings (python)
    if (lang === "python" && (code.slice(i, i + 3) === '"""' || code.slice(i, i + 3) === "'''")) {
      const q = code.slice(i, i + 3);
      let end = code.indexOf(q, i + 3);
      end = end === -1 ? code.length : end + 3;
      tokens.push({ type: "string", value: code.slice(i, end) });
      i = end;
      continue;
    }

    // Strings
    if (code[i] === '"' || code[i] === "'") {
      const q = code[i];
      let j = i + 1;
      while (j < code.length && code[j] !== q) {
        if (code[j] === "\\") j++;
        j++;
      }
      j = Math.min(j + 1, code.length);
      tokens.push({ type: "string", value: code.slice(i, j) });
      i = j;
      continue;
    }

    // f-string prefix
    if (lang === "python" && (code[i] === "f" || code[i] === "F") && (code[i + 1] === '"' || code[i + 1] === "'")) {
      const q = code[i + 1];
      let j = i + 2;
      while (j < code.length && code[j] !== q) {
        if (code[j] === "\\") j++;
        j++;
      }
      j = Math.min(j + 1, code.length);
      tokens.push({ type: "string", value: code.slice(i, j) });
      i = j;
      continue;
    }

    // Decorator (python)
    if (lang === "python" && code[i] === "@" && (i === 0 || code[i - 1] === "\n")) {
      let end = i + 1;
      while (end < code.length && /[\w.]/.test(code[end])) end++;
      tokens.push({ type: "decorator", value: code.slice(i, end) });
      i = end;
      continue;
    }

    // Numbers
    if (/\d/.test(code[i]) && (i === 0 || !/[\w]/.test(code[i - 1]))) {
      let j = i;
      while (j < code.length && /[\d.eE_xXbBoO]/.test(code[j])) j++;
      tokens.push({ type: "number", value: code.slice(i, j) });
      i = j;
      continue;
    }

    // Words (identifiers / keywords)
    if (/[a-zA-Z_]/.test(code[i])) {
      let j = i;
      while (j < code.length && /[\w]/.test(code[j])) j++;
      const word = code.slice(i, j);
      const upper = word.toUpperCase();

      let type = "text";
      if (lang === "sql") {
        if (SQL_KEYWORDS.includes(upper)) type = "keyword";
        else if (SQL_BUILTINS.includes(upper)) type = "builtin";
        else if (code[j] === "(") type = "function";
      } else if (lang === "python") {
        if (PYTHON_KEYWORDS.includes(word)) type = "keyword";
        else if (PYTHON_CONSTANTS.includes(word)) type = "constant";
        else if (PYTHON_BUILTINS.includes(word)) type = "builtin";
        else if (code[j] === "(") type = "function";
        else if (word === word.toUpperCase() && word.length > 1) type = "constant";
      }
      tokens.push({ type, value: word });
      i = j;
      continue;
    }

    // Operators
    if ("=<>!+-*/%&|^~".includes(code[i])) {
      let j = i;
      while (j < code.length && "=<>!+-*/%&|^~".includes(code[j])) j++;
      tokens.push({ type: "operator", value: code.slice(i, j) });
      i = j;
      continue;
    }

    // Punctuation
    if ("()[]{},:;.".includes(code[i])) {
      tokens.push({ type: "punctuation", value: code[i] });
      i++;
      continue;
    }

    // Newlines
    if (code[i] === "\n") {
      tokens.push({ type: "newline", value: "\n" });
      i++;
      continue;
    }

    // Whitespace
    if (/\s/.test(code[i])) {
      let j = i;
      while (j < code.length && /\s/.test(code[j]) && code[j] !== "\n") j++;
      tokens.push({ type: "ws", value: code.slice(i, j) });
      i = j;
      continue;
    }

    tokens.push({ type: "text", value: code[i] });
    i++;
  }

  return tokens;
}

function escapeHtml(text: string) {
  return text.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

function isCommentHeader(value: string): boolean {
  const trimmed = value.replace(/^(--|#)\s*/, "");
  // Header-style comments start with === or a short description (no code-like content)
  return /^={3,}/.test(trimmed) || (/^[A-ZÀÂÉÈÊËÏÎÔÙÛÜŸÇŒÆ]/.test(trimmed) && trimmed.length > 3 && !trimmed.includes("("));
}

function renderTokens(tokens: Token[]) {
  return tokens.map((t, i) => {
    const escaped = escapeHtml(t.value);
    switch (t.type) {
      case "keyword":
        return <span key={i} className="cb-keyword">{escaped}</span>;
      case "builtin":
        return <span key={i} className="cb-builtin">{escaped}</span>;
      case "function":
        return <span key={i} className="cb-fn">{escaped}</span>;
      case "string":
        return <span key={i} className="cb-string">{escaped}</span>;
      case "number":
        return <span key={i} className="cb-num">{escaped}</span>;
      case "constant":
        return <span key={i} className="cb-const">{escaped}</span>;
      case "comment":
        return (
          <span key={i} className={isCommentHeader(t.value) ? "cb-comment-header" : "cb-comment"}>
            {escaped}
          </span>
        );
      case "decorator":
        return <span key={i} className="cb-decorator">{escaped}</span>;
      case "operator":
        return <span key={i} className="cb-op">{escaped}</span>;
      case "punctuation":
        return <span key={i} className="cb-punct">{escaped}</span>;
      default:
        return <span key={i}>{escaped}</span>;
    }
  });
}

/* ───── Main component ───── */

export default function CodeBlock({ code, language, title }: CodeBlockProps) {
  const [copied, setCopied] = useState(false);

  const handleCopy = async () => {
    await navigator.clipboard.writeText(code);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  const tokens = tokenize(code, language);

  // Split tokens into lines for line numbering
  const lines: Token[][] = [[]];
  tokens.forEach((t) => {
    if (t.type === "newline") {
      lines.push([]);
    } else {
      lines[lines.length - 1].push(t);
    }
  });

  const lang = language.toLowerCase();
  const langLabel = lang === "python" ? "Python" : lang === "sql" ? "SQL" : language;

  return (
    <div className="cb-root my-5">
      {/* ── Header ── */}
      <div className="cb-header">
        <div className="flex items-center gap-2.5">
          <div className="flex gap-1.5">
            <span className="w-2.5 h-2.5 rounded-full bg-[#ff5f57]" />
            <span className="w-2.5 h-2.5 rounded-full bg-[#febc2e]" />
            <span className="w-2.5 h-2.5 rounded-full bg-[#28c840]" />
          </div>
          <div className="flex items-center gap-1.5 ml-1 text-[11px] text-gray-400">
            <LangIcon lang={lang} />
            <span className="font-medium tracking-wide">{title || langLabel}</span>
          </div>
        </div>
        <button
          onClick={handleCopy}
          className="cb-copy-btn"
          aria-label="Copier le code"
        >
          {copied ? (
            <>
              <svg className="w-3.5 h-3.5 text-emerald-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
              </svg>
              <span className="text-emerald-400">Copié !</span>
            </>
          ) : (
            <>
              <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
                  d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z" />
              </svg>
              Copier
            </>
          )}
        </button>
      </div>

      {/* ── Code body ── */}
      <div className="cb-body">
        <table className="cb-table">
          <tbody>
            {lines.map((lineTokens, idx) => {
              const isEmpty = lineTokens.length === 0 || lineTokens.every((t) => t.type === "ws" && t.value.trim() === "");
              return (
                <tr key={idx} className={isEmpty ? "cb-line cb-line-empty" : "cb-line"}>
                  <td className="cb-ln">{idx + 1}</td>
                  <td className="cb-code">
                    {lineTokens.length > 0 ? renderTokens(lineTokens) : " "}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}
