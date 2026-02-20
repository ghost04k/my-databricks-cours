"use client";

import { useState } from "react";
import Link from "next/link";
import InfoBox from "@/components/InfoBox";

interface Question {
  id: number;
  module: string;
  question: string;
  options: string[];
  answer: number;
  explanation: string;
}

const questions: Question[] = [
  // ===================== MODULE 1 (4 questions) =====================
  {
    id: 1,
    module: "Module 1 — Prise en main",
    question:
      "Quelle est la différence principale entre un cluster All-Purpose et un cluster de Job ?",
    options: [
      "Le coût est le même",
      "Les clusters de Job sont moins chers et créés/détruits automatiquement",
      "All-Purpose est plus rapide",
      "Les clusters de Job ne supportent pas Python",
    ],
    answer: 1,
    explanation:
      "Les clusters de Job sont créés spécifiquement pour un job et détruits à la fin, ce qui coûte moins cher. Les clusters All-Purpose restent actifs et sont facturés tant qu'ils tournent.",
  },
  {
    id: 2,
    module: "Module 1 — Prise en main",
    question:
      "Quelle commande magique permet d'exécuter du SQL dans un notebook Python ?",
    options: ["%run", "%sql", "%query", "!sql"],
    answer: 1,
    explanation:
      "La commande magique %sql permet d'exécuter du SQL directement dans une cellule d'un notebook Python. C'est une fonctionnalité propre aux notebooks Databricks.",
  },
  {
    id: 3,
    module: "Module 1 — Prise en main",
    question: "La commande %run permet de :",
    options: [
      "Exécuter une requête SQL",
      "Exécuter un autre notebook dans le contexte actuel",
      "Redémarrer le cluster",
      "Exécuter du code Python",
    ],
    answer: 1,
    explanation:
      "%run exécute un autre notebook dans le même contexte. Les variables, fonctions et imports définis dans le notebook appelé deviennent disponibles dans le notebook appelant.",
  },
  {
    id: 4,
    module: "Module 1 — Prise en main",
    question:
      "Quel est l'avantage principal de l'architecture Lakehouse ?",
    options: [
      "Elle est plus rapide que tout",
      "Elle combine les avantages du data warehouse et du data lake",
      "Elle ne nécessite pas de cluster",
      "Elle est uniquement pour le ML",
    ],
    answer: 1,
    explanation:
      "L'architecture Lakehouse combine la fiabilité et les performances du data warehouse avec la flexibilité et le faible coût du data lake, grâce à Delta Lake.",
  },
  // ===================== MODULE 2 (6 questions) =====================
  {
    id: 5,
    module: "Module 2 — SQL & Tables",
    question:
      "Que se passe-t-il quand on DROP une table managée ?",
    options: [
      "Seules les métadonnées sont supprimées",
      "Les données et les métadonnées sont supprimées",
      "La table est simplement renommée",
      "Rien",
    ],
    answer: 1,
    explanation:
      "Pour une table managée (par défaut dans Databricks), un DROP TABLE supprime à la fois les métadonnées du metastore ET les fichiers de données sous-jacents. Pour une table externe, seules les métadonnées sont supprimées.",
  },
  {
    id: 6,
    module: "Module 2 — SQL & Tables",
    question:
      "Quelle est la différence entre CREATE VIEW et CREATE TEMP VIEW ?",
    options: [
      "Aucune différence",
      "TEMP VIEW est persistée dans le metastore",
      "TEMP VIEW disparaît à la fin de la session",
      "TEMP VIEW est plus rapide",
    ],
    answer: 2,
    explanation:
      "Une vue temporaire (TEMP VIEW) n'existe que pendant la durée de la session Spark. Elle n'est pas enregistrée dans le metastore et disparaît quand le cluster est arrêté ou la session terminée.",
  },
  {
    id: 7,
    module: "Module 2 — SQL & Tables",
    question: "MERGE INTO est utilisé principalement pour :",
    options: [
      "Supprimer des données",
      "Créer des vues",
      "Réaliser des upserts (insert + update)",
      "Compresser les fichiers",
    ],
    answer: 2,
    explanation:
      "MERGE INTO combine INSERT et UPDATE en une seule opération atomique. Si la ligne existe, elle est mise à jour ; sinon, elle est insérée. C'est l'opération d'upsert standard en SQL.",
  },
  {
    id: 8,
    module: "Module 2 — SQL & Tables",
    question: "La fonction FILTER dans Spark SQL est :",
    options: [
      "Un filtre WHERE classique",
      "Une fonction d'ordre supérieur pour filtrer les éléments d'un tableau",
      "Un filtre sur les partitions",
      "Une fonction de fenêtrage",
    ],
    answer: 1,
    explanation:
      "FILTER est une fonction d'ordre supérieur (higher-order function) qui s'applique aux colonnes de type ARRAY. Elle permet de filtrer les éléments d'un tableau selon une condition, sans exploser le tableau.",
  },
  {
    id: 9,
    module: "Module 2 — SQL & Tables",
    question:
      "COPY INTO est idempotent, cela signifie que :",
    options: [
      "Il supprime les données avant de copier",
      "Il ne copiera pas les fichiers déjà traités",
      "Il écrase toujours les données",
      "Il vérifie la qualité des données",
    ],
    answer: 1,
    explanation:
      "L'idempotence de COPY INTO signifie qu'il garde un suivi des fichiers déjà ingérés. Si vous relancez la commande, les fichiers déjà traités seront ignorés, évitant les doublons.",
  },
  {
    id: 10,
    module: "Module 2 — SQL & Tables",
    question:
      "Quelle commande permet de voir si une table est managée ou externe ?",
    options: [
      "SHOW TABLE",
      "DESCRIBE EXTENDED",
      "SELECT METADATA FROM",
      "TABLE INFO",
    ],
    answer: 1,
    explanation:
      "DESCRIBE EXTENDED affiche des informations détaillées sur une table, y compris son type (MANAGED ou EXTERNAL), son emplacement, son schéma, ses propriétés et ses statistiques.",
  },
  // ===================== MODULE 3 (6 questions) =====================
  {
    id: 11,
    module: "Module 3 — Streaming",
    question:
      "Quel trigger remplace le trigger `once` déprécié dans Structured Streaming ?",
    options: ["processingTime", "availableNow", "continuous", "immediate"],
    answer: 1,
    explanation:
      "Le trigger availableNow remplace trigger(once=True) qui est déprécié. Il traite toutes les données disponibles en un seul micro-batch et est plus efficace car il peut créer plusieurs micro-batches si nécessaire.",
  },
  {
    id: 12,
    module: "Module 3 — Streaming",
    question: "Auto Loader utilise quel format dans readStream ?",
    options: ["json", "delta", "cloudFiles", "autoloader"],
    answer: 2,
    explanation:
      'Auto Loader utilise le format "cloudFiles" dans readStream. C\'est le format spécifique de Databricks qui permet l\'ingestion automatique et incrémentale de fichiers depuis le stockage cloud.',
  },
  {
    id: 13,
    module: "Module 3 — Streaming",
    question:
      "Dans l'architecture Medallion, la couche Bronze contient :",
    options: [
      "Des données agrégées",
      "Des données nettoyées",
      "Des données brutes sans transformation",
      "Des KPIs business",
    ],
    answer: 2,
    explanation:
      "La couche Bronze contient les données brutes telles qu'elles ont été ingérées, sans transformation. La couche Silver contient les données nettoyées et la couche Gold les données agrégées/business.",
  },
  {
    id: 14,
    module: "Module 3 — Streaming",
    question:
      "Le checkpointing en Structured Streaming sert à :",
    options: [
      "Accélérer les requêtes",
      "Garantir le traitement exactly-once en cas de panne",
      "Compresser les données",
      "Créer des sauvegardes",
    ],
    answer: 1,
    explanation:
      "Le checkpoint enregistre l'état du stream et les offsets des données déjà traitées. En cas de panne, le stream peut reprendre exactement là où il s'est arrêté, garantissant un traitement exactly-once.",
  },
  {
    id: 15,
    module: "Module 3 — Streaming",
    question:
      "La colonne _rescued_data dans Auto Loader contient :",
    options: [
      "Les données dupliquées",
      "Les données dont le type ne correspond pas au schéma inféré",
      "Les données supprimées",
      "Les métadonnées",
    ],
    answer: 1,
    explanation:
      "Lorsque Auto Loader rencontre des données qui ne correspondent pas au schéma inféré (mauvais type, colonne manquante), elles sont placées dans la colonne _rescued_data au lieu d'être perdues.",
  },
  {
    id: 16,
    module: "Module 3 — Streaming",
    question:
      "Quel output mode utiliser pour des agrégations complètes en streaming ?",
    options: ["append", "update", "complete", "overwrite"],
    answer: 2,
    explanation:
      "Le mode complete réécrit l'intégralité du résultat à chaque micro-batch. C'est le mode requis pour les agrégations complètes (sans watermark). Le mode append ne fonctionne pas pour les agrégations non bornées.",
  },
  // ===================== MODULE 4 (8 questions) =====================
  {
    id: 17,
    module: "Module 4 — DLT & Jobs",
    question: "Dans DLT, le préfixe LIVE. sert à :",
    options: [
      "Indiquer une table en temps réel",
      "Référencer une autre table DLT dans le même pipeline",
      "Créer une table temporaire",
      "Activer le streaming",
    ],
    answer: 1,
    explanation:
      "Le préfixe LIVE. est utilisé dans les pipelines DLT pour référencer d'autres tables ou vues définies dans le même pipeline. Par exemple, SELECT * FROM LIVE.bronze_table.",
  },
  {
    id: 18,
    module: "Module 4 — DLT & Jobs",
    question:
      "Quelle expectation DLT supprime les lignes invalides ?",
    options: [
      "EXPECT",
      "EXPECT ON VIOLATION WARN",
      "EXPECT ON VIOLATION DROP ROW",
      "EXPECT ON VIOLATION FAIL UPDATE",
    ],
    answer: 2,
    explanation:
      "EXPECT ... ON VIOLATION DROP ROW supprime silencieusement les lignes qui ne respectent pas la contrainte. WARN les laisse passer avec un avertissement. FAIL UPDATE arrête le pipeline.",
  },
  {
    id: 19,
    module: "Module 4 — DLT & Jobs",
    question: "En mode Triggered, un pipeline DLT :",
    options: [
      "Tourne en continu",
      "Se déclenche manuellement ou selon un schedule",
      "Est automatiquement lancé par Unity Catalog",
      "Fonctionne uniquement en Python",
    ],
    answer: 1,
    explanation:
      "En mode Triggered, le pipeline traite les données disponibles puis s'arrête. Il peut être déclenché manuellement ou programmé via un schedule (cron). En mode Continuous, il reste actif en permanence.",
  },
  {
    id: 20,
    module: "Module 4 — DLT & Jobs",
    question: "La fonction event_log() permet de :",
    options: [
      "Créer des logs applicatifs",
      "Monitorer et analyser les résultats d'un pipeline DLT",
      "Enregistrer les erreurs SQL",
      "Configurer les alertes",
    ],
    answer: 1,
    explanation:
      "event_log() est une fonction spécifique à DLT qui expose les événements du pipeline : métriques, résultats des expectations, erreurs, durées. Elle est essentielle pour le monitoring.",
  },
  {
    id: 21,
    module: "Module 4 — DLT & Jobs",
    question:
      "dbutils.jobs.taskValues.set() permet de :",
    options: [
      "Configurer les paramètres du cluster",
      "Passer des données entre les tâches d'un job",
      "Définir des variables d'environnement",
      "Créer des widgets",
    ],
    answer: 1,
    explanation:
      "dbutils.jobs.taskValues.set() permet à une tâche d'un job multi-tâches de transmettre des valeurs à d'autres tâches en aval. C'est le mécanisme de communication inter-tâches dans Databricks Jobs.",
  },
  {
    id: 22,
    module: "Module 4 — DLT & Jobs",
    question:
      "Quel est l'avantage d'un Job Cluster vs All-Purpose ?",
    options: [
      "Plus de mémoire",
      "Moins cher car créé/détruit automatiquement pour chaque job",
      "Plus rapide",
      "Supporte plus de langages",
    ],
    answer: 1,
    explanation:
      "Les Job Clusters sont créés au démarrage du job et détruits à la fin. Vous ne payez que le temps d'exécution réel. Les All-Purpose restent actifs et coûtent plus cher à l'heure.",
  },
  {
    id: 23,
    module: "Module 4 — DLT & Jobs",
    question:
      "La fonctionnalité Repair Run dans Databricks Jobs permet de :",
    options: [
      "Réparer les fichiers corrompus",
      "Relancer uniquement les tâches échouées",
      "Corriger le code automatiquement",
      "Restaurer un job supprimé",
    ],
    answer: 1,
    explanation:
      "Repair Run permet de relancer un job en ne ré-exécutant que les tâches qui ont échoué. Les tâches réussies ne sont pas relancées, ce qui économise du temps et des ressources.",
  },
  {
    id: 24,
    module: "Module 4 — DLT & Jobs",
    question:
      "CREATE OR REFRESH STREAMING TABLE dans DLT crée :",
    options: [
      "Une table classique",
      "Une table qui traite les données de façon incrémentale",
      "Une vue temporaire",
      "Un fichier Parquet",
    ],
    answer: 1,
    explanation:
      "CREATE OR REFRESH STREAMING TABLE crée une table DLT qui traite les données de manière incrémentale. À chaque exécution, seules les nouvelles données sont traitées, en utilisant Structured Streaming en arrière-plan.",
  },
  // ===================== MODULE 5 (6 questions) =====================
  {
    id: 25,
    module: "Module 5 — Gouvernance",
    question:
      "Le namespace à 3 niveaux dans Unity Catalog est :",
    options: [
      "database.schema.table",
      "catalog.schema.table",
      "metastore.catalog.table",
      "workspace.database.table",
    ],
    answer: 1,
    explanation:
      "Unity Catalog utilise un namespace à 3 niveaux : catalog.schema.table (ou catalog.schema.view). Le catalog est le niveau le plus haut, suivi du schema, puis de l'objet (table, vue, fonction).",
  },
  {
    id: 26,
    module: "Module 5 — Gouvernance",
    question: "Le privilège USAGE dans Unity Catalog :",
    options: [
      "Est hérité automatiquement aux niveaux inférieurs",
      "Doit être accordé à chaque niveau séparément",
      "Donne accès en lecture aux données",
      "Est uniquement pour les admins",
    ],
    answer: 1,
    explanation:
      "Le privilège USAGE doit être accordé à chaque niveau du namespace séparément (catalog, puis schema). Il ne donne pas accès aux données, il permet seulement de \"traverser\" le niveau pour atteindre les objets en dessous.",
  },
  {
    id: 27,
    module: "Module 5 — Gouvernance",
    question:
      "Pour masquer une colonne selon le rôle de l'utilisateur, on utilise :",
    options: [
      "GRANT MASK",
      "Une vue dynamique avec CASE et is_member()",
      "ALTER COLUMN SET VISIBLE FALSE",
      "DENY SELECT ON COLUMN",
    ],
    answer: 1,
    explanation:
      "Les vues dynamiques avec CASE WHEN is_member('group') permettent d'afficher ou masquer des colonnes selon l'appartenance de l'utilisateur à un groupe. C'est le pattern standard pour le row/column-level security.",
  },
  {
    id: 28,
    module: "Module 5 — Gouvernance",
    question: "current_user() dans Unity Catalog retourne :",
    options: [
      "Le nom du cluster",
      "L'email de l'utilisateur connecté",
      "Le nom du workspace",
      "L'ID du job",
    ],
    answer: 1,
    explanation:
      "current_user() retourne l'adresse email de l'utilisateur authentifié qui exécute la requête. C'est utilisé dans les vues dynamiques pour le contrôle d'accès au niveau des lignes.",
  },
  {
    id: 29,
    module: "Module 5 — Gouvernance",
    question:
      "Un External Location dans Unity Catalog est :",
    options: [
      "Un cluster externe",
      "Un emplacement de stockage cloud enregistré et gouverné",
      "Une connexion API externe",
      "Un lien vers un autre workspace",
    ],
    answer: 1,
    explanation:
      "Un External Location est un chemin de stockage cloud (S3, ADLS, GCS) enregistré dans Unity Catalog. Il est associé à un Storage Credential et permet de gouverner l'accès au stockage externe.",
  },
  {
    id: 30,
    module: "Module 5 — Gouvernance",
    question: "ALL PRIVILEGES dans GRANT inclut :",
    options: [
      "Uniquement SELECT et MODIFY",
      "Tous les privilèges actuels et futurs",
      "Uniquement les privilèges de lecture",
      "Les privilèges d'admin",
    ],
    answer: 1,
    explanation:
      "ALL PRIVILEGES accorde tous les privilèges disponibles pour l'objet concerné, y compris ceux qui pourraient être ajoutés dans le futur. C'est un raccourci puissant, à utiliser avec précaution.",
  },
];

function getResultMessage(percentage: number) {
  if (percentage >= 90) {
    return {
      emoji: "🏆",
      title: "Excellent !",
      message: "Vous êtes prêt pour la certification !",
      color: "bg-emerald-50 border-emerald-300 text-emerald-800",
    };
  }
  if (percentage >= 70) {
    return {
      emoji: "👍",
      title: "Bien !",
      message:
        "Révisez les modules où vous avez fait des erreurs avant de passer la certification.",
      color: "bg-blue-50 border-blue-300 text-blue-800",
    };
  }
  if (percentage >= 50) {
    return {
      emoji: "📚",
      title: "Continuez à étudier.",
      message:
        "Relisez attentivement les leçons, en particulier les modules où vous avez des lacunes.",
      color: "bg-amber-50 border-amber-300 text-amber-800",
    };
  }
  return {
    emoji: "💪",
    title: "Reprenez le programme depuis le début.",
    message:
      "Courage ! Revoyez chaque module en détail et refaites les exercices pratiques avant de retenter le quiz.",
    color: "bg-red-50 border-red-300 text-red-800",
  };
}

export default function QuizCertificationPage() {
  const [currentQuestion, setCurrentQuestion] = useState(0);
  const [selectedAnswer, setSelectedAnswer] = useState<number | null>(null);
  const [showResult, setShowResult] = useState(false);
  const [score, setScore] = useState(0);
  const [isFinished, setIsFinished] = useState(false);

  const q = questions[currentQuestion];
  const progress = ((currentQuestion + (showResult ? 1 : 0)) / questions.length) * 100;

  const handleValidate = () => {
    if (selectedAnswer === null) return;
    setShowResult(true);
    if (selectedAnswer === q.answer) {
      setScore((s) => s + 1);
    }
  };

  const handleNext = () => {
    if (currentQuestion + 1 >= questions.length) {
      setIsFinished(true);
    } else {
      setCurrentQuestion((c) => c + 1);
      setSelectedAnswer(null);
      setShowResult(false);
    }
  };

  const handleRestart = () => {
    setCurrentQuestion(0);
    setSelectedAnswer(null);
    setShowResult(false);
    setScore(0);
    setIsFinished(false);
  };

  const percentage = Math.round((score / questions.length) * 100);
  const result = getResultMessage(percentage);

  return (
    <div className="min-h-[calc(100vh-4rem)]">
      {/* Hero */}
      <div className="relative bg-gradient-to-br from-[#1b3a4b] via-[#2d5f7a] to-[#1b3a4b] text-white overflow-hidden">
        <div className="absolute inset-0 opacity-10">
          <div className="absolute top-10 left-10 w-72 h-72 bg-[#ff3621] rounded-full blur-3xl" />
          <div className="absolute bottom-10 right-10 w-96 h-96 bg-blue-400 rounded-full blur-3xl" />
        </div>
        <div className="relative max-w-4xl mx-auto px-6 py-14 lg:py-18">
          <div className="flex items-center gap-3 mb-4">
            <span className="inline-flex items-center px-3 py-1 rounded-full text-xs font-semibold bg-purple-400/20 text-purple-200 border border-purple-400/30">
              Tous niveaux
            </span>
            <span className="text-sm text-white/70">⏱ 2 heures</span>
            <span className="text-sm text-white/70">📝 30 questions</span>
          </div>
          <h1 className="text-3xl lg:text-4xl font-extrabold mb-3">
            🎓 Quiz de Préparation à la Certification
          </h1>
          <p className="text-lg text-white/80 max-w-2xl leading-relaxed">
            30 questions couvrant les 5 modules du programme. Testez vos
            connaissances et identifiez les points à réviser avant
            l&apos;examen.
          </p>
        </div>
      </div>

      {/* Contenu */}
      <div className="max-w-3xl mx-auto px-6 py-10 lg:px-10">
        {/* Navigation */}
        <div className="flex flex-wrap gap-3 mb-10">
          <Link
            href="/exercices"
            className="inline-flex items-center gap-2 text-sm text-[#1b3a4b] hover:text-[#ff3621] transition-colors"
          >
            ← Tous les exercices
          </Link>
          <span className="text-gray-300">|</span>
          <Link
            href="/programme"
            className="inline-flex items-center gap-2 text-sm text-[#1b3a4b] hover:text-[#ff3621] transition-colors"
          >
            📅 Programme complet
          </Link>
        </div>

        {/* ============ QUIZ TERMINÉ ============ */}
        {isFinished ? (
          <div className="space-y-8">
            <div
              className={`rounded-xl border-2 p-8 text-center ${result.color}`}
            >
              <div className="text-6xl mb-4">{result.emoji}</div>
              <h2 className="text-2xl font-extrabold mb-2">{result.title}</h2>
              <p className="text-lg mb-6">{result.message}</p>
              <div className="text-4xl font-black">
                {score} / {questions.length}
              </div>
              <p className="text-lg font-semibold mt-1">{percentage}%</p>
            </div>

            {/* Détail par module */}
            <div className="bg-gray-50 rounded-xl border border-gray-200 p-6">
              <h3 className="text-lg font-bold text-[#1b3a4b] mb-4">
                📊 Détail par module
              </h3>
              <div className="space-y-3">
                {[
                  { name: "Module 1 — Prise en main", range: [0, 3] },
                  { name: "Module 2 — SQL & Tables", range: [4, 9] },
                  { name: "Module 3 — Streaming", range: [10, 15] },
                  { name: "Module 4 — DLT & Jobs", range: [16, 23] },
                  { name: "Module 5 — Gouvernance", range: [24, 29] },
                ].map((mod) => {
                  const total = mod.range[1] - mod.range[0] + 1;
                  return (
                    <div key={mod.name} className="flex items-center gap-3">
                      <span className="text-sm font-medium text-gray-700 w-52">
                        {mod.name}
                      </span>
                      <div className="flex-1 h-3 bg-gray-200 rounded-full overflow-hidden">
                        <div
                          className="h-full bg-[#ff3621] rounded-full transition-all"
                          style={{ width: "0%" }}
                        />
                      </div>
                      <span className="text-sm text-gray-500 w-12 text-right">
                        / {total}
                      </span>
                    </div>
                  );
                })}
              </div>
            </div>

            <div className="flex flex-col sm:flex-row gap-4 justify-center">
              <button
                onClick={handleRestart}
                className="px-6 py-3 rounded-lg bg-[#1b3a4b] text-white font-semibold hover:bg-[#2d5f7a] transition-colors"
              >
                🔄 Recommencer le quiz
              </button>
              <Link
                href="/programme"
                className="px-6 py-3 rounded-lg bg-[#ff3621] text-white font-semibold hover:bg-[#e0301d] transition-colors text-center"
              >
                📅 Revoir le programme
              </Link>
            </div>
          </div>
        ) : (
          /* ============ QUESTION EN COURS ============ */
          <div className="space-y-6">
            {/* Barre de progression */}
            <div>
              <div className="flex justify-between items-center mb-2">
                <span className="text-sm font-semibold text-[#1b3a4b]">
                  Question {currentQuestion + 1} / {questions.length}
                </span>
                <span className="text-xs text-gray-500">{q.module}</span>
              </div>
              <div className="w-full h-3 bg-gray-200 rounded-full overflow-hidden">
                <div
                  className="h-full bg-gradient-to-r from-[#ff3621] to-[#ff6b4a] rounded-full transition-all duration-500"
                  style={{ width: `${progress}%` }}
                />
              </div>
            </div>

            {/* Question */}
            <div className="bg-white rounded-xl border border-gray-200 shadow-sm p-6">
              <h2 className="text-lg font-bold text-[#1b3a4b] mb-6">
                {q.question}
              </h2>

              <div className="space-y-3">
                {q.options.map((option, idx) => {
                  let borderColor = "border-gray-200 hover:border-[#2d5f7a]";
                  let bgColor = "bg-white hover:bg-gray-50";
                  let textColor = "text-gray-700";
                  let indicator = (
                    <span className="w-7 h-7 flex items-center justify-center rounded-full border-2 border-gray-300 text-xs font-bold text-gray-400">
                      {String.fromCharCode(65 + idx)}
                    </span>
                  );

                  if (showResult) {
                    if (idx === q.answer) {
                      borderColor = "border-emerald-400";
                      bgColor = "bg-emerald-50";
                      textColor = "text-emerald-800";
                      indicator = (
                        <span className="w-7 h-7 flex items-center justify-center rounded-full bg-emerald-500 text-white text-xs font-bold">
                          ✓
                        </span>
                      );
                    } else if (idx === selectedAnswer) {
                      borderColor = "border-red-400";
                      bgColor = "bg-red-50";
                      textColor = "text-red-800";
                      indicator = (
                        <span className="w-7 h-7 flex items-center justify-center rounded-full bg-red-500 text-white text-xs font-bold">
                          ✗
                        </span>
                      );
                    } else {
                      bgColor = "bg-gray-50";
                      textColor = "text-gray-400";
                    }
                  } else if (selectedAnswer === idx) {
                    borderColor = "border-[#2d5f7a]";
                    bgColor = "bg-[#1b3a4b]/5";
                    textColor = "text-[#1b3a4b]";
                    indicator = (
                      <span className="w-7 h-7 flex items-center justify-center rounded-full bg-[#1b3a4b] text-white text-xs font-bold">
                        {String.fromCharCode(65 + idx)}
                      </span>
                    );
                  }

                  return (
                    <button
                      key={idx}
                      onClick={() => !showResult && setSelectedAnswer(idx)}
                      disabled={showResult}
                      className={`w-full flex items-center gap-4 p-4 rounded-lg border-2 transition-all text-left ${borderColor} ${bgColor} ${
                        showResult ? "cursor-default" : "cursor-pointer"
                      }`}
                    >
                      {indicator}
                      <span className={`text-sm font-medium ${textColor}`}>
                        {option}
                      </span>
                    </button>
                  );
                })}
              </div>
            </div>

            {/* Explication */}
            {showResult && (
              <div
                className={`rounded-lg border p-4 ${
                  selectedAnswer === q.answer
                    ? "bg-emerald-50 border-emerald-200"
                    : "bg-red-50 border-red-200"
                }`}
              >
                <p
                  className={`text-sm font-semibold mb-1 ${
                    selectedAnswer === q.answer
                      ? "text-emerald-800"
                      : "text-red-800"
                  }`}
                >
                  {selectedAnswer === q.answer
                    ? "✅ Bonne réponse !"
                    : "❌ Mauvaise réponse"}
                </p>
                <p
                  className={`text-sm ${
                    selectedAnswer === q.answer
                      ? "text-emerald-700"
                      : "text-red-700"
                  }`}
                >
                  {q.explanation}
                </p>
              </div>
            )}

            {/* Boutons */}
            <div className="flex justify-end gap-3">
              {!showResult ? (
                <button
                  onClick={handleValidate}
                  disabled={selectedAnswer === null}
                  className={`px-6 py-2.5 rounded-lg text-sm font-semibold transition-colors ${
                    selectedAnswer === null
                      ? "bg-gray-200 text-gray-400 cursor-not-allowed"
                      : "bg-[#ff3621] text-white hover:bg-[#e0301d]"
                  }`}
                >
                  Valider
                </button>
              ) : (
                <button
                  onClick={handleNext}
                  className="px-6 py-2.5 rounded-lg text-sm font-semibold bg-[#1b3a4b] text-white hover:bg-[#2d5f7a] transition-colors"
                >
                  {currentQuestion + 1 >= questions.length
                    ? "Voir les résultats"
                    : "Question suivante →"}
                </button>
              )}
            </div>

            {/* Info */}
            {currentQuestion === 0 && !showResult && (
              <InfoBox type="tip" title="Conseil">
                Prenez le temps de réfléchir à chaque question. Ce quiz couvre
                les 5 modules du programme : prise en main, SQL &amp; tables,
                streaming, DLT &amp; jobs, et gouvernance.
              </InfoBox>
            )}
          </div>
        )}

        {/* Navigation bas de page */}
        <div className="mt-16 pt-8 border-t border-gray-200 flex flex-col sm:flex-row items-center justify-between gap-4">
          <Link
            href="/exercices"
            className="inline-flex items-center gap-2 text-sm text-[#1b3a4b] hover:text-[#ff3621] font-medium transition-colors"
          >
            ← Retour aux exercices
          </Link>
          <Link
            href="/exercices/delta-lake-avance"
            className="inline-flex items-center gap-2 px-5 py-2.5 rounded-lg bg-[#ff3621] text-white text-sm font-semibold hover:bg-[#e0301d] transition-colors"
          >
            Exercices Delta Lake Avancé →
          </Link>
        </div>
      </div>
    </div>
  );
}
