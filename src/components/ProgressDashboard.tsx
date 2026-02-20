"use client";

import { useProgress } from "./ProgressContext";

export default function ProgressDashboard() {
  const {
    getOverallProgress,
    getModuleProgress,
    completedCount,
    totalLessons,
    moduleNames,
    progress,
  } = useProgress();

  const overall = getOverallProgress();
  const quizCount = Object.keys(progress.quizScores).length;
  const avgScore =
    quizCount > 0
      ? Math.round(
          (Object.values(progress.quizScores).reduce(
            (sum, s) => sum + (s.correct / s.total) * 100,
            0
          ) /
            quizCount)
        )
      : 0;

  return (
    <div className="bg-white rounded-2xl border border-gray-200 shadow-sm p-6 mb-8">
      {/* Header */}
      <div className="flex items-center justify-between mb-4">
        <h3 className="text-lg font-bold text-[#1b3a4b] flex items-center gap-2">
          📊 Votre Progression
        </h3>
        <span className="text-sm text-gray-500">
          {completedCount}/{totalLessons} leçons
        </span>
      </div>

      {/* Overall progress bar */}
      <div className="mb-6">
        <div className="flex items-center justify-between mb-1.5">
          <span className="text-sm font-medium text-gray-700">
            Avancement global
          </span>
          <span className="text-sm font-bold text-[#00a972]">{overall}%</span>
        </div>
        <div className="w-full bg-gray-100 rounded-full h-4 overflow-hidden">
          <div
            className="h-full rounded-full transition-all duration-700 ease-out relative"
            style={{
              width: `${overall}%`,
              background:
                overall < 30
                  ? "linear-gradient(90deg, #ef4444, #f97316)"
                  : overall < 70
                  ? "linear-gradient(90deg, #f97316, #eab308)"
                  : "linear-gradient(90deg, #22c55e, #00a972)",
            }}
          >
            {overall > 10 && (
              <div className="absolute inset-0 bg-white/20 animate-pulse rounded-full" />
            )}
          </div>
        </div>
      </div>

      {/* Module progress bars */}
      <div className="space-y-3">
        {[1, 2, 3, 4, 5].map((moduleNum) => {
          const mp = getModuleProgress(moduleNum);
          return (
            <div key={moduleNum}>
              <div className="flex items-center justify-between mb-1">
                <span className="text-xs font-medium text-gray-600 truncate pr-4">
                  Module {moduleNum} : {moduleNames[moduleNum]}
                </span>
                <span
                  className={`text-xs font-bold ${
                    mp === 100 ? "text-[#00a972]" : "text-gray-400"
                  }`}
                >
                  {mp === 100 ? "✅" : `${mp}%`}
                </span>
              </div>
              <div className="w-full bg-gray-100 rounded-full h-2">
                <div
                  className="h-full rounded-full transition-all duration-500"
                  style={{
                    width: `${mp}%`,
                    backgroundColor: mp === 100 ? "#00a972" : "#3b82f6",
                  }}
                />
              </div>
            </div>
          );
        })}
      </div>

      {/* Stats */}
      <div className="mt-6 pt-4 border-t border-gray-100 grid grid-cols-3 gap-4 text-center">
        <div>
          <p className="text-2xl font-bold text-[#1b3a4b]">{completedCount}</p>
          <p className="text-xs text-gray-500">Leçons terminées</p>
        </div>
        <div>
          <p className="text-2xl font-bold text-[#1b3a4b]">{quizCount}</p>
          <p className="text-xs text-gray-500">Quiz complétés</p>
        </div>
        <div>
          <p className="text-2xl font-bold text-[#1b3a4b]">
            {quizCount > 0 ? `${avgScore}%` : "—"}
          </p>
          <p className="text-xs text-gray-500">Score moyen</p>
        </div>
      </div>
    </div>
  );
}
