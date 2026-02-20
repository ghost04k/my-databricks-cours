"use client";

import { useProgress } from "./ProgressContext";

interface LessonCompleteButtonProps {
  lessonSlug: string;
}

export default function LessonCompleteButton({
  lessonSlug,
}: LessonCompleteButtonProps) {
  const { isLessonComplete, markLessonComplete, getQuizScore } = useProgress();
  const done = isLessonComplete(lessonSlug);
  const quizScore = getQuizScore(lessonSlug);

  return (
    <div className="mt-10 p-6 rounded-2xl border-2 border-dashed border-gray-300 bg-gray-50 text-center">
      {done ? (
        <div>
          <span className="text-4xl">🎉</span>
          <p className="mt-2 text-lg font-bold text-[#00a972]">
            Leçon complétée !
          </p>
          {quizScore && (
            <p className="text-sm text-gray-500 mt-1">
              Quiz : {quizScore.correct}/{quizScore.total} (
              {Math.round((quizScore.correct / quizScore.total) * 100)}%)
            </p>
          )}
        </div>
      ) : (
        <div>
          <span className="text-4xl">📖</span>
          <p className="mt-2 text-sm text-gray-500 mb-3">
            Vous avez terminé la lecture ? Marquez cette leçon comme complétée
            ou validez le quiz pour progresser.
          </p>
          <button
            onClick={() => markLessonComplete(lessonSlug)}
            className="inline-flex items-center gap-2 px-6 py-2.5 bg-[#00a972] text-white font-medium rounded-lg hover:bg-[#008f5f] transition-colors shadow"
          >
            ✓ Marquer comme complétée
          </button>
        </div>
      )}
    </div>
  );
}
