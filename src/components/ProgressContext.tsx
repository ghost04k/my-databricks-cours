"use client";

import { createContext, useContext, useState, useEffect, ReactNode } from "react";

// All lesson slugs in order
const ALL_LESSONS = [
  "1-0-creation-clusters",
  "1-1-bases-notebooks",
  "2-1-bases-donnees-tables",
  "2-2-vues-ctes",
  "2-3-transformations-donnees",
  "2-4-fonctions-sql-avancees",
  "3-1-structured-streaming",
  "3-2-auto-loader",
  "3-3-architecture-multi-hop",
  "4-1-delta-live-tables",
  "4-2-resultats-pipeline",
  "4-3-orchestration-jobs",
  "5-1-unity-catalog",
  "5-2-gestion-permissions",
];

const MODULE_NAMES: Record<number, string> = {
  1: "Databricks Lakehouse Platform",
  2: "ELT avec Spark SQL et Python",
  3: "Traitement Incrémental",
  4: "Pipelines de Production",
  5: "Gouvernance des Données",
};

interface QuizScore {
  correct: number;
  total: number;
  date: string;
}

interface ProgressData {
  completedLessons: string[];
  quizScores: Record<string, QuizScore>;
  exercisesDone: Record<string, boolean>;
}

interface ProgressContextType {
  progress: ProgressData;
  markLessonComplete: (slug: string) => void;
  isLessonComplete: (slug: string) => boolean;
  saveQuizScore: (lessonSlug: string, correct: number, total: number) => void;
  getQuizScore: (lessonSlug: string) => QuizScore | null;
  markExerciseDone: (exerciseId: string) => void;
  isExerciseDone: (exerciseId: string) => boolean;
  getOverallProgress: () => number;
  getModuleProgress: (moduleNumber: number) => number;
  resetProgress: () => void;
  totalLessons: number;
  completedCount: number;
  allLessons: string[];
  moduleNames: Record<number, string>;
}

const ProgressContext = createContext<ProgressContextType | undefined>(undefined);

const STORAGE_KEY = "databricks-cours-progress";

const defaultProgress: ProgressData = {
  completedLessons: [],
  quizScores: {},
  exercisesDone: {},
};

export function ProgressProvider({ children }: { children: ReactNode }) {
  const [progress, setProgress] = useState<ProgressData>(defaultProgress);
  const [loaded, setLoaded] = useState(false);

  // Load from localStorage
  useEffect(() => {
    try {
      const stored = localStorage.getItem(STORAGE_KEY);
      if (stored) {
        const parsed = JSON.parse(stored);
        setProgress({
          completedLessons: parsed.completedLessons || [],
          quizScores: parsed.quizScores || {},
          exercisesDone: parsed.exercisesDone || {},
        });
      }
    } catch {
      // ignore
    }
    setLoaded(true);
  }, []);

  // Save to localStorage
  useEffect(() => {
    if (loaded) {
      localStorage.setItem(STORAGE_KEY, JSON.stringify(progress));
    }
  }, [progress, loaded]);

  const markLessonComplete = (slug: string) => {
    setProgress((prev) => {
      if (prev.completedLessons.includes(slug)) return prev;
      return { ...prev, completedLessons: [...prev.completedLessons, slug] };
    });
  };

  const isLessonComplete = (slug: string) =>
    progress.completedLessons.includes(slug);

  const saveQuizScore = (lessonSlug: string, correct: number, total: number) => {
    setProgress((prev) => ({
      ...prev,
      quizScores: {
        ...prev.quizScores,
        [lessonSlug]: {
          correct,
          total,
          date: new Date().toISOString(),
        },
      },
    }));
  };

  const getQuizScore = (lessonSlug: string) =>
    progress.quizScores[lessonSlug] || null;

  const markExerciseDone = (exerciseId: string) => {
    setProgress((prev) => ({
      ...prev,
      exercisesDone: { ...prev.exercisesDone, [exerciseId]: true },
    }));
  };

  const isExerciseDone = (exerciseId: string) =>
    !!progress.exercisesDone[exerciseId];

  const getOverallProgress = () =>
    ALL_LESSONS.length > 0
      ? Math.round((progress.completedLessons.length / ALL_LESSONS.length) * 100)
      : 0;

  const getModuleProgress = (moduleNumber: number) => {
    const moduleLessons = ALL_LESSONS.filter((l) =>
      l.startsWith(`${moduleNumber}-`)
    );
    if (moduleLessons.length === 0) return 0;
    const completed = moduleLessons.filter((l) =>
      progress.completedLessons.includes(l)
    ).length;
    return Math.round((completed / moduleLessons.length) * 100);
  };

  const resetProgress = () => {
    setProgress(defaultProgress);
    localStorage.removeItem(STORAGE_KEY);
  };

  return (
    <ProgressContext.Provider
      value={{
        progress,
        markLessonComplete,
        isLessonComplete,
        saveQuizScore,
        getQuizScore,
        markExerciseDone,
        isExerciseDone,
        getOverallProgress,
        getModuleProgress,
        resetProgress,
        totalLessons: ALL_LESSONS.length,
        completedCount: progress.completedLessons.length,
        allLessons: ALL_LESSONS,
        moduleNames: MODULE_NAMES,
      }}
    >
      {children}
    </ProgressContext.Provider>
  );
}

export function useProgress() {
  const context = useContext(ProgressContext);
  if (!context) {
    throw new Error("useProgress must be used within a ProgressProvider");
  }
  return context;
}
