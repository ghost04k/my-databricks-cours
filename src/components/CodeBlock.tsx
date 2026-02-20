"use client";

import { useState } from "react";

interface CodeBlockProps {
  code: string;
  language: string;
  title?: string;
}

const SQL_KEYWORDS = [
  "SELECT",
  "FROM",
  "WHERE",
  "INSERT",
  "INTO",
  "UPDATE",
  "DELETE",
  "CREATE",
  "DROP",
  "ALTER",
  "TABLE",
  "DATABASE",
  "VIEW",
  "INDEX",
  "JOIN",
  "INNER",
  "LEFT",
  "RIGHT",
  "OUTER",
  "ON",
  "AND",
  "OR",
  "NOT",
  "IN",
  "EXISTS",
  "BETWEEN",
  "LIKE",
  "IS",
  "NULL",
  "AS",
  "ORDER",
  "BY",
  "GROUP",
  "HAVING",
  "LIMIT",
  "OFFSET",
  "UNION",
  "ALL",
  "DISTINCT",
  "CASE",
  "WHEN",
  "THEN",
  "ELSE",
  "END",
  "SET",
  "VALUES",
  "WITH",
  "TEMPORARY",
  "TEMP",
  "IF",
  "REPLACE",
  "USING",
  "DELTA",
  "LOCATION",
  "OPTIONS",
  "COMMENT",
  "DESCRIBE",
  "SHOW",
  "USE",
  "SCHEMA",
  "CATALOG",
  "GRANT",
  "REVOKE",
  "DENY",
];

const PYTHON_KEYWORDS = [
  "def",
  "class",
  "import",
  "from",
  "return",
  "if",
  "elif",
  "else",
  "for",
  "while",
  "try",
  "except",
  "finally",
  "with",
  "as",
  "yield",
  "lambda",
  "pass",
  "break",
  "continue",
  "raise",
  "in",
  "not",
  "and",
  "or",
  "is",
  "None",
  "True",
  "False",
  "async",
  "await",
  "print",
];

function highlightCode(code: string, language: string): string {
  const lang = language.toLowerCase();
  let result = code
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");

  // Comments
  if (lang === "sql") {
    result = result.replace(
      /(--.*$)/gm,
      '<span class="comment">$1</span>'
    );
  } else if (lang === "python") {
    result = result.replace(
      /(#.*$)/gm,
      '<span class="comment">$1</span>'
    );
  }

  // Strings
  result = result.replace(
    /("(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*')/g,
    '<span class="string">$1</span>'
  );

  // Numbers
  result = result.replace(
    /\b(\d+(?:\.\d+)?)\b/g,
    '<span class="number">$1</span>'
  );

  // Keywords
  const keywords = lang === "sql" ? SQL_KEYWORDS : lang === "python" ? PYTHON_KEYWORDS : [];
  if (keywords.length > 0) {
    const keywordPattern = new RegExp(
      `\\b(${keywords.join("|")})\\b`,
      lang === "sql" ? "gi" : "g"
    );
    result = result.replace(keywordPattern, '<span class="keyword">$1</span>');
  }

  // Functions (word followed by parenthesis)
  result = result.replace(
    /\b([a-zA-Z_]\w*)\s*(?=\()/g,
    '<span class="function">$1</span>'
  );

  return result;
}

export default function CodeBlock({ code, language, title }: CodeBlockProps) {
  const [copied, setCopied] = useState(false);

  const handleCopy = async () => {
    await navigator.clipboard.writeText(code);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  const highlighted = highlightCode(code, language);

  return (
    <div className="rounded-xl overflow-hidden my-4 shadow-md">
      {/* Title bar */}
      <div className="flex items-center justify-between px-4 py-2.5 bg-[#181825] text-gray-400 text-xs">
        <div className="flex items-center gap-2">
          <div className="flex gap-1.5">
            <span className="w-3 h-3 rounded-full bg-[#f38ba8]" />
            <span className="w-3 h-3 rounded-full bg-[#f9e2af]" />
            <span className="w-3 h-3 rounded-full bg-[#a6e3a1]" />
          </div>
          <span className="ml-2 font-medium text-gray-300">
            {title || language.toUpperCase()}
          </span>
        </div>
        <button
          onClick={handleCopy}
          className="flex items-center gap-1.5 px-2.5 py-1 rounded-md hover:bg-white/10 transition-colors text-gray-400 hover:text-gray-200"
        >
          {copied ? (
            <>
              <svg className="w-4 h-4 text-green-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
              </svg>
              <span className="text-green-400">Copié !</span>
            </>
          ) : (
            <>
              <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2}
                  d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z"
                />
              </svg>
              Copier
            </>
          )}
        </button>
      </div>

      {/* Code content */}
      <pre className="code-block !rounded-t-none !mt-0 !mb-0">
        <code dangerouslySetInnerHTML={{ __html: highlighted }} />
      </pre>
    </div>
  );
}
