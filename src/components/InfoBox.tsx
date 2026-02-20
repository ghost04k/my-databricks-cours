import React from "react";

interface InfoBoxProps {
  type: "info" | "tip" | "warning" | "important";
  title: string;
  children: React.ReactNode;
}

const config = {
  info: {
    bg: "bg-blue-50",
    border: "border-blue-200",
    titleColor: "text-blue-800",
    textColor: "text-blue-700",
    icon: (
      <svg className="w-5 h-5 text-blue-500" fill="currentColor" viewBox="0 0 24 24">
        <path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm1 15h-2v-6h2v6zm0-8h-2V7h2v2z" />
      </svg>
    ),
  },
  tip: {
    bg: "bg-emerald-50",
    border: "border-emerald-200",
    titleColor: "text-emerald-800",
    textColor: "text-emerald-700",
    icon: (
      <svg className="w-5 h-5 text-emerald-500" fill="currentColor" viewBox="0 0 24 24">
        <path d="M9 21c0 .55.45 1 1 1h4c.55 0 1-.45 1-1v-1H9v1zm3-19C8.14 2 5 5.14 5 9c0 2.38 1.19 4.47 3 5.74V17c0 .55.45 1 1 1h6c.55 0 1-.45 1-1v-2.26c1.81-1.27 3-3.36 3-5.74 0-3.86-3.14-7-7-7z" />
      </svg>
    ),
  },
  warning: {
    bg: "bg-amber-50",
    border: "border-amber-200",
    titleColor: "text-amber-800",
    textColor: "text-amber-700",
    icon: (
      <svg className="w-5 h-5 text-amber-500" fill="currentColor" viewBox="0 0 24 24">
        <path d="M1 21h22L12 2 1 21zm12-3h-2v-2h2v2zm0-4h-2v-4h2v4z" />
      </svg>
    ),
  },
  important: {
    bg: "bg-red-50",
    border: "border-red-200",
    titleColor: "text-red-800",
    textColor: "text-red-700",
    icon: (
      <svg className="w-5 h-5 text-red-500" fill="currentColor" viewBox="0 0 24 24">
        <path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm-2 15l-5-5 1.41-1.41L10 14.17l7.59-7.59L19 8l-9 9z" />
      </svg>
    ),
  },
};

export default function InfoBox({ type, title, children }: InfoBoxProps) {
  const style = config[type];

  return (
    <div
      className={`${style.bg} ${style.border} border rounded-xl p-4 my-4`}
    >
      <div className="flex items-center gap-2 mb-2">
        {style.icon}
        <h4 className={`font-semibold text-sm ${style.titleColor}`}>
          {title}
        </h4>
      </div>
      <div className={`text-sm leading-relaxed ${style.textColor}`}>
        {children}
      </div>
    </div>
  );
}
