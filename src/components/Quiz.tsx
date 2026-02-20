"use client";

import { useState } from "react";
import { useProgress } from "./ProgressContext";

export interface QuizQuestion {
  question: string;
  options: string[];
  correctIndex: number;
  explanation: string;
}

interface QuizProps {
  lessonSlug: string;
  title?: string;
  questions: QuizQuestion[];
}

export default function Quiz({ lessonSlug, title, questions }: QuizProps) {
  const { saveQuizScore, getQuizScore, markLessonComplete } = useProgress();

  const [currentQ, setCurrentQ] = useState(0);
  const [selected, setSelected] = useState<number | null>(null);
  const [showExplanation, setShowExplanation] = useState(false);
  const [answers, setAnswers] = useState<(number | null)[]>(
    new Array(questions.length).fill(null)
  );
  const [finished, setFinished] = useState(false);

  const previousScore = getQuizScore(lessonSlug);
  const question = questions[currentQ];

  const handleSelect = (idx: number) => {
    if (showExplanation) return;
    setSelected(idx);
  };

  const handleValidate = () => {
    if (selected === null) return;
    const newAnswers = [...answers];
    newAnswers[currentQ] = selected;
    setAnswers(newAnswers);
    setShowExplanation(true);
  };

  const handleNext = () => {
    if (currentQ < questions.length - 1) {
      setCurrentQ(currentQ + 1);
      setSelected(null);
      setShowExplanation(false);
    } else {
      // Finish quiz
      const correct = answers.reduce(
        (sum: number, a, i) => sum + (a === questions[i].correctIndex ? 1 : 0),
        0
      );
      saveQuizScore(lessonSlug, correct, questions.length);
      if (correct >= questions.length * 0.7) {
        markLessonComplete(lessonSlug);
      }
      setFinished(true);
    }
  };

  const handleRetry = () => {
    setCurrentQ(0);
    setSelected(null);
    setShowExplanation(false);
    setAnswers(new Array(questions.length).fill(null));
    setFinished(false);
  };

  if (finished) {
    const correct = answers.reduce(
      (sum: number, a, i) => sum + (a === questions[i].correctIndex ? 1 : 0),
      0
    );
    const pct = Math.round((correct / questions.length) * 100);
    const passed = pct >= 70;

    return (
      <div className="mt-12 rounded-2xl border-2 border-gray-200 bg-white overflow-hidden">
        <div
          className={`px-6 py-4 ${
            passed
              ? "bg-gradient-to-r from-green-500 to-emerald-600"
              : "bg-gradient-to-r from-orange-500 to-red-500"
          } text-white`}
        >
          <h3 className="text-xl font-bold">
            {passed ? "🎉 Bravo !" : "📚 Continuez vos efforts !"}
          </h3>
        </div>
        <div className="p-6 text-center">
          <div className="inline-flex items-center justify-center w-24 h-24 rounded-full border-4 mb-4"
            style={{
              borderColor: passed ? "#00a972" : "#f97316",
            }}
          >
            <span
              className="text-3xl font-bold"
              style={{ color: passed ? "#00a972" : "#f97316" }}
            >
              {pct}%
            </span>
          </div>
          <p className="text-lg text-gray-700 mb-1">
            <strong>{correct}</strong> bonne{correct > 1 ? "s" : ""} réponse
            {correct > 1 ? "s" : ""} sur <strong>{questions.length}</strong>
          </p>
          <p className="text-sm text-gray-500 mb-6">
            {passed
              ? "Leçon marquée comme complétée ! ✅"
              : "Il faut 70% pour valider la leçon. Réessayez !"}
          </p>

          {/* Answer review */}
          <div className="text-left space-y-3 mb-6">
            {questions.map((q, i) => {
              const userAnswer = answers[i];
              const isCorrect = userAnswer === q.correctIndex;
              return (
                <div
                  key={i}
                  className={`p-3 rounded-lg border text-sm ${
                    isCorrect
                      ? "bg-green-50 border-green-200"
                      : "bg-red-50 border-red-200"
                  }`}
                >
                  <div className="flex items-start gap-2">
                    <span>{isCorrect ? "✅" : "❌"}</span>
                    <div>
                      <p className="font-medium text-gray-800">{q.question}</p>
                      {!isCorrect && (
                        <p className="text-xs text-gray-600 mt-1">
                          Réponse correcte : {q.options[q.correctIndex]}
                        </p>
                      )}
                    </div>
                  </div>
                </div>
              );
            })}
          </div>

          <button
            onClick={handleRetry}
            className="px-6 py-2.5 bg-[#1b3a4b] text-white font-medium rounded-lg hover:bg-[#2d5f7a] transition-colors"
          >
            🔄 Recommencer le quiz
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="mt-12 rounded-2xl border-2 border-gray-200 bg-white overflow-hidden">
      {/* Header */}
      <div className="px-6 py-4 bg-gradient-to-r from-[#1b3a4b] to-[#2d5f7a] text-white">
        <div className="flex items-center justify-between">
          <h3 className="text-lg font-bold flex items-center gap-2">
            📝 {title || "Quiz de la leçon"}
          </h3>
          <span className="text-sm bg-white/20 px-3 py-1 rounded-full">
            {currentQ + 1}/{questions.length}
          </span>
        </div>
        {/* Progress dots */}
        <div className="flex gap-1.5 mt-3">
          {questions.map((_, i) => (
            <div
              key={i}
              className={`h-1.5 rounded-full transition-all flex-1 ${
                i < currentQ
                  ? answers[i] === questions[i].correctIndex
                    ? "bg-green-400"
                    : "bg-red-400"
                  : i === currentQ
                  ? "bg-white"
                  : "bg-white/30"
              }`}
            />
          ))}
        </div>
        {previousScore && (
          <p className="text-xs text-white/70 mt-2">
            Meilleur score : {previousScore.correct}/{previousScore.total} (
            {Math.round((previousScore.correct / previousScore.total) * 100)}
            %)
          </p>
        )}
      </div>

      {/* Question */}
      <div className="p-6">
        <p className="text-lg font-semibold text-gray-800 mb-5">
          {question.question}
        </p>

        {/* Options */}
        <div className="space-y-3">
          {question.options.map((opt, i) => {
            let style =
              "border-gray-200 bg-gray-50 hover:bg-blue-50 hover:border-blue-300";
            if (selected === i && !showExplanation)
              style =
                "border-blue-500 bg-blue-50 ring-2 ring-blue-200";
            if (showExplanation) {
              if (i === question.correctIndex)
                style =
                  "border-green-500 bg-green-50 ring-2 ring-green-200";
              else if (i === selected)
                style =
                  "border-red-500 bg-red-50 ring-2 ring-red-200";
              else style = "border-gray-200 bg-gray-50 opacity-50";
            }

            return (
              <button
                key={i}
                onClick={() => handleSelect(i)}
                disabled={showExplanation}
                className={`w-full text-left p-4 rounded-xl border-2 transition-all ${style}`}
              >
                <div className="flex items-center gap-3">
                  <span
                    className={`w-8 h-8 rounded-full flex items-center justify-center text-sm font-bold border-2 shrink-0 ${
                      showExplanation && i === question.correctIndex
                        ? "bg-green-500 text-white border-green-500"
                        : showExplanation && i === selected
                        ? "bg-red-500 text-white border-red-500"
                        : selected === i
                        ? "bg-blue-500 text-white border-blue-500"
                        : "bg-white text-gray-500 border-gray-300"
                    }`}
                  >
                    {showExplanation && i === question.correctIndex
                      ? "✓"
                      : showExplanation && i === selected
                      ? "✗"
                      : String.fromCharCode(65 + i)}
                  </span>
                  <span className="text-sm text-gray-800">{opt}</span>
                </div>
              </button>
            );
          })}
        </div>

        {/* Explanation */}
        {showExplanation && (
          <div className="mt-4 p-4 rounded-xl bg-blue-50 border border-blue-200">
            <p className="text-sm font-semibold text-blue-800 mb-1">
              💡 Explication
            </p>
            <p className="text-sm text-blue-700">{question.explanation}</p>
          </div>
        )}

        {/* Actions */}
        <div className="mt-6 flex justify-end gap-3">
          {!showExplanation ? (
            <button
              onClick={handleValidate}
              disabled={selected === null}
              className={`px-6 py-2.5 rounded-lg font-medium transition-all ${
                selected === null
                  ? "bg-gray-200 text-gray-400 cursor-not-allowed"
                  : "bg-[#ff3621] text-white hover:bg-[#e02e1a] shadow"
              }`}
            >
              Valider
            </button>
          ) : (
            <button
              onClick={handleNext}
              className="px-6 py-2.5 bg-[#1b3a4b] text-white font-medium rounded-lg hover:bg-[#2d5f7a] transition-colors shadow"
            >
              {currentQ < questions.length - 1
                ? "Question suivante →"
                : "Voir les résultats 🏆"}
            </button>
          )}
        </div>
      </div>
    </div>
  );
}
