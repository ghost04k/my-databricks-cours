"use client";

import { useState } from "react";
import { useProgress } from "./ProgressContext";
import CodeBlock from "./CodeBlock";

export interface LessonExercise {
  id: string;
  title: string;
  description: string;
  difficulty: "facile" | "moyen" | "difficile";
  type: "code" | "reflexion" | "pratique";
  prompt: string;
  hints: string[];
  solution: {
    code?: string;
    language?: string;
    explanation: string;
  };
}

interface LessonExercisesProps {
  lessonSlug: string;
  exercises: LessonExercise[];
}

export default function LessonExercises({
  lessonSlug,
  exercises,
}: LessonExercisesProps) {
  const { markExerciseDone, isExerciseDone } = useProgress();
  const [showSolution, setShowSolution] = useState<Record<string, boolean>>({});
  const [showHints, setShowHints] = useState<Record<string, boolean>>({});

  const doneCount = exercises.filter((e) =>
    isExerciseDone(`${lessonSlug}-${e.id}`)
  ).length;

  const difficultyColor = {
    facile: "bg-green-100 text-green-700 border-green-300",
    moyen: "bg-amber-100 text-amber-700 border-amber-300",
    difficile: "bg-red-100 text-red-700 border-red-300",
  };

  const typeIcon = {
    code: "💻",
    reflexion: "🤔",
    pratique: "🔧",
  };

  return (
    <div className="mt-12">
      <div className="flex items-center justify-between mb-6">
        <h2 className="text-2xl font-bold text-[#1b3a4b] flex items-center gap-2">
          🏋️ Exercices pratiques
        </h2>
        <span className="text-sm font-medium text-gray-500 bg-gray-100 px-3 py-1 rounded-full">
          {doneCount}/{exercises.length} terminés
        </span>
      </div>

      <div className="space-y-6">
        {exercises.map((ex, idx) => {
          const exerciseKey = `${lessonSlug}-${ex.id}`;
          const done = isExerciseDone(exerciseKey);

          return (
            <div
              key={ex.id}
              className={`rounded-xl border-2 overflow-hidden transition-all ${
                done
                  ? "border-green-300 bg-green-50/30"
                  : "border-gray-200 bg-white"
              }`}
            >
              {/* Exercise header */}
              <div className="px-6 py-4 border-b border-gray-100 flex items-center justify-between gap-4">
                <div className="flex items-center gap-3 min-w-0">
                  <span className="text-xl shrink-0">
                    {done ? "✅" : typeIcon[ex.type]}
                  </span>
                  <div className="min-w-0">
                    <h3 className="font-bold text-gray-800 truncate">
                      Exercice {idx + 1} : {ex.title}
                    </h3>
                    <p className="text-sm text-gray-500 mt-0.5">
                      {ex.description}
                    </p>
                  </div>
                </div>
                <span
                  className={`shrink-0 text-xs font-semibold px-2.5 py-0.5 rounded-full border ${difficultyColor[ex.difficulty]}`}
                >
                  {ex.difficulty}
                </span>
              </div>

              {/* Exercise content */}
              <div className="p-6">
                <div className="bg-gray-50 rounded-lg p-4 mb-4">
                  <p className="text-sm font-semibold text-gray-700 mb-2">
                    📋 Consigne :
                  </p>
                  <p className="text-sm text-gray-600 whitespace-pre-line">
                    {ex.prompt}
                  </p>
                </div>

                {/* Hints */}
                <button
                  onClick={() =>
                    setShowHints((prev) => ({
                      ...prev,
                      [ex.id]: !prev[ex.id],
                    }))
                  }
                  className="text-sm text-blue-600 hover:text-blue-800 font-medium mb-3 flex items-center gap-1"
                >
                  {showHints[ex.id] ? "🙈 Masquer" : "💡 Voir"} les indices (
                  {ex.hints.length})
                </button>

                {showHints[ex.id] && (
                  <div className="mb-4 space-y-2">
                    {ex.hints.map((hint, i) => (
                      <div
                        key={i}
                        className="flex items-start gap-2 text-sm text-blue-700 bg-blue-50 rounded-lg px-3 py-2"
                      >
                        <span className="shrink-0 font-bold">#{i + 1}</span>
                        <span>{hint}</span>
                      </div>
                    ))}
                  </div>
                )}

                {/* Solution toggle */}
                <button
                  onClick={() =>
                    setShowSolution((prev) => ({
                      ...prev,
                      [ex.id]: !prev[ex.id],
                    }))
                  }
                  className="inline-flex items-center gap-2 px-4 py-2 bg-[#1b3a4b] text-white text-sm font-medium rounded-lg hover:bg-[#2d5f7a] transition-colors"
                >
                  {showSolution[ex.id]
                    ? "🙈 Masquer la solution"
                    : "👁️ Voir la solution"}
                </button>

                {showSolution[ex.id] && (
                  <div className="mt-4 p-5 bg-gray-50 rounded-xl border border-gray-200">
                    {ex.solution.code && (
                      <CodeBlock
                        language={ex.solution.language || "sql"}
                        code={ex.solution.code}
                      />
                    )}
                    <p className="mt-3 text-sm text-gray-700">
                      {ex.solution.explanation}
                    </p>
                  </div>
                )}

                {/* Mark as done */}
                {!done && (
                  <button
                    onClick={() => markExerciseDone(exerciseKey)}
                    className="mt-4 ml-2 inline-flex items-center gap-2 px-4 py-2 bg-[#00a972] text-white text-sm font-medium rounded-lg hover:bg-[#008f5f] transition-colors"
                  >
                    ✓ Marquer comme fait
                  </button>
                )}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
