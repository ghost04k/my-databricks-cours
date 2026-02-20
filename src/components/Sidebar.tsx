"use client";

import { useState } from "react";
import Link from "next/link";
import { useProgress } from "./ProgressContext";

interface SidebarProps {
  currentPath: string;
}

interface Lesson {
  title: string;
  slug: string;
}

interface Module {
  title: string;
  lessons: Lesson[];
}

const modules: Module[] = [
  {
    title: "Module 1 : Databricks Lakehouse Platform",
    lessons: [
      { title: "1.0 Création de Clusters", slug: "1-0-creation-clusters" },
      { title: "1.1 Bases des Notebooks", slug: "1-1-bases-notebooks" },
    ],
  },
  {
    title: "Module 2 : ELT avec Spark SQL et Python",
    lessons: [
      {
        title: "2.1 Bases de données et Tables",
        slug: "2-1-bases-donnees-tables",
      },
      { title: "2.2 Vues et CTEs", slug: "2-2-vues-ctes" },
      {
        title: "2.3 Transformations de données",
        slug: "2-3-transformations-donnees",
      },
      {
        title: "2.4 Fonctions SQL avancées",
        slug: "2-4-fonctions-sql-avancees",
      },
    ],
  },
  {
    title: "Module 3 : Traitement Incrémental",
    lessons: [
      {
        title: "3.1 Structured Streaming",
        slug: "3-1-structured-streaming",
      },
      { title: "3.2 Auto Loader", slug: "3-2-auto-loader" },
      {
        title: "3.3 Architecture Multi-Hop",
        slug: "3-3-architecture-multi-hop",
      },
    ],
  },
  {
    title: "Module 4 : Pipelines de Production",
    lessons: [
      { title: "4.1 Delta Live Tables", slug: "4-1-delta-live-tables" },
      {
        title: "4.2 Résultats de Pipeline",
        slug: "4-2-resultats-pipeline",
      },
      {
        title: "4.3 Orchestration de Jobs",
        slug: "4-3-orchestration-jobs",
      },
    ],
  },
  {
    title: "Module 5 : Gouvernance des Données",
    lessons: [
      { title: "5.1 Unity Catalog", slug: "5-1-unity-catalog" },
      {
        title: "5.2 Gestion des permissions",
        slug: "5-2-gestion-permissions",
      },
    ],
  },
];

export default function Sidebar({ currentPath }: SidebarProps) {
  const { isLessonComplete, getOverallProgress } = useProgress();
  const overallProgress = getOverallProgress();

  const [openModules, setOpenModules] = useState<Record<number, boolean>>(() => {
    const initial: Record<number, boolean> = {};
    modules.forEach((mod, idx) => {
      const isActive = mod.lessons.some(
        (lesson) => currentPath === `/modules/${lesson.slug}`
      );
      initial[idx] = isActive;
    });
    return initial;
  });

  const toggleModule = (index: number) => {
    setOpenModules((prev) => ({ ...prev, [index]: !prev[index] }));
  };

  return (
    <aside className="w-72 shrink-0 bg-white border-r border-[var(--color-border)] h-[calc(100vh-4rem)] sticky top-16 overflow-y-auto hidden lg:block">
      <div className="p-4">
        {/* Progression globale compacte */}
        <div className="mb-4 px-3 py-2 bg-gray-50 rounded-lg">
          <div className="flex items-center justify-between text-xs text-[var(--color-text-light)] mb-1">
            <span>Progression</span>
            <span className="font-semibold text-[#ff3621]">{overallProgress}%</span>
          </div>
          <div className="w-full bg-gray-200 rounded-full h-2">
            <div
              className="bg-gradient-to-r from-[#ff3621] to-[#00a972] h-2 rounded-full transition-all duration-500"
              style={{ width: `${overallProgress}%` }}
            />
          </div>
        </div>
        <h2 className="text-xs font-semibold uppercase tracking-wider text-[var(--color-text-light)] mb-4">
          Sommaire du cours
        </h2>
        <nav className="space-y-1">
          {modules.map((mod, idx) => (
            <div key={idx}>
              {/* Module header */}
              <button
                onClick={() => toggleModule(idx)}
                className="w-full flex items-center justify-between px-3 py-2.5 text-sm font-semibold text-[var(--color-secondary)] hover:bg-gray-50 rounded-lg transition-colors"
              >
                <span className="text-left leading-tight flex items-center gap-2">
                  {mod.lessons.every((l) => isLessonComplete(l.slug)) && (
                    <span className="text-[#00a972] text-base">✅</span>
                  )}
                  {mod.title}
                </span>
                <svg
                  className={`w-4 h-4 shrink-0 ml-2 transition-transform duration-200 ${
                    openModules[idx] ? "rotate-90" : ""
                  }`}
                  fill="none"
                  stroke="currentColor"
                  viewBox="0 0 24 24"
                >
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    strokeWidth={2}
                    d="M9 5l7 7-7 7"
                  />
                </svg>
              </button>

              {/* Lessons */}
              {openModules[idx] && (
                <div className="ml-3 border-l-2 border-gray-100 pl-3 mt-1 mb-2 space-y-0.5">
                  {mod.lessons.map((lesson) => {
                    const href = `/modules/${lesson.slug}`;
                    const isActive = currentPath === href;
                    return (
                      <Link
                        key={lesson.slug}
                        href={href}
                        className={`flex items-center gap-2 px-3 py-2 text-sm rounded-lg transition-colors ${
                          isActive
                            ? "bg-[#ff3621]/10 text-[#ff3621] font-medium border-l-2 border-[#ff3621] -ml-[calc(0.75rem+2px)] pl-[calc(0.75rem+2px)]"
                            : isLessonComplete(lesson.slug)
                              ? "text-[#00a972] hover:bg-gray-50"
                              : "text-[var(--color-text-light)] hover:text-[var(--color-text)] hover:bg-gray-50"
                        }`}
                      >
                        {isLessonComplete(lesson.slug) ? (
                          <span className="shrink-0 w-4 h-4 rounded-full bg-[#00a972] flex items-center justify-center">
                            <svg className="w-3 h-3 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={3} d="M5 13l4 4L19 7" /></svg>
                          </span>
                        ) : (
                          <span className="shrink-0 w-4 h-4 rounded-full border-2 border-gray-300" />
                        )}
                        {lesson.title}
                      </Link>
                    );
                  })}
                </div>
              )}
            </div>
          ))}
        </nav>
      </div>
    </aside>
  );
}
