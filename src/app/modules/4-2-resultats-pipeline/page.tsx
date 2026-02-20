"use client";

import Link from "next/link";
import Sidebar from "@/components/Sidebar";
import CodeBlock from "@/components/CodeBlock";
import InfoBox from "@/components/InfoBox";
import Quiz from "@/components/Quiz";
import type { QuizQuestion } from "@/components/Quiz";
import LessonExercises from "@/components/LessonExercises";
import type { LessonExercise } from "@/components/LessonExercises";
import LessonCompleteButton from "@/components/LessonCompleteButton";

const quizQuestions: QuizQuestion[] = [
  {
    question: "Comment accéder à l'event log d'un pipeline DLT ?",
    options: ["SELECT * FROM pipeline_log", "SELECT * FROM event_log('pipeline_name')", "dbutils.pipeline.getLog()", "SHOW LOGS pipeline_name"],
    correctIndex: 1,
    explanation: "La fonction event_log() permet de requêter le journal d'événements d'un pipeline DLT.",
  },
  {
    question: "Qui gère les clusters d'un pipeline DLT ?",
    options: ["L'utilisateur", "L'administrateur", "DLT gère ses propres clusters automatiquement", "Le job scheduler"],
    correctIndex: 2,
    explanation: "DLT crée et gère ses propres clusters automatiquement, l'utilisateur n'a pas besoin de les configurer.",
  },
  {
    question: "Quel type d'événement contient les métriques de qualité ?",
    options: ["dataset_definition", "flow_progress", "cluster_status", "maintenance"],
    correctIndex: 1,
    explanation: "Les événements flow_progress contiennent les métriques de traitement et les résultats des expectations de qualité.",
  },
  {
    question: "Où sont stockés les fichiers Delta d'un pipeline DLT ?",
    options: ["Dans /tmp/dlt/", "Dans le répertoire tables/ du storage location du pipeline", "Dans le metastore Hive", "Dans un bucket séparé"],
    correctIndex: 1,
    explanation: "Le storage location contient system/ (event log), tables/ (Delta tables), et autoloader/ (schema tracking).",
  },
  {
    question: "Comment visualiser le DAG d'un pipeline DLT ?",
    options: ["Avec dbutils.pipeline.dag()", "Dans l'interface graphique du pipeline DLT", "Avec SHOW DAG pipeline", "Ce n'est pas possible"],
    correctIndex: 1,
    explanation: "L'interface DLT de Databricks affiche automatiquement le DAG avec les dépendances entre les tables.",
  },
];

const exercises: LessonExercise[] = [
  {
    id: "analyser-event-log",
    title: "Analyser l'event log",
    description: "Écrivez des requêtes SQL pour extraire les métriques de qualité, le nombre de lignes et les événements d'erreur depuis l'event log.",
    difficulty: "moyen",
    type: "code",
    prompt: "Écrivez trois requêtes SQL exploitant l'event log d'un pipeline DLT : 1) extraire les métriques de qualité des expectations, 2) compter le nombre de lignes produites par table, 3) lister les événements d'erreur.",
    hints: [
      "Utilisez event_log('pipeline_name') comme source",
      "Filtrez sur event_type = 'flow_progress' pour les métriques",
      "Utilisez la notation details:flow_progress.data_quality pour le JSON",
    ],
    solution: {
      code: `-- 1. Métriques de qualité des expectations\nSELECT\n  timestamp,\n  details:flow_progress.data_quality.expectations.name AS contrainte,\n  details:flow_progress.data_quality.expectations.passed_records AS valides,\n  details:flow_progress.data_quality.expectations.failed_records AS invalides\nFROM event_log("my_pipeline")\nWHERE event_type = 'flow_progress'\n  AND details:flow_progress.data_quality IS NOT NULL\nORDER BY timestamp DESC;\n\n-- 2. Nombre de lignes produites par table\nSELECT\n  details:flow_progress.metrics.num_output_rows AS lignes_produites,\n  details:flow_progress.data_quality.expectations.dataset AS table_cible\nFROM event_log("my_pipeline")\nWHERE event_type = 'flow_progress';\n\n-- 3. Événements d'erreur\nSELECT timestamp, event_type, message\nFROM event_log("my_pipeline")\nWHERE level = 'ERROR'\nORDER BY timestamp DESC;`,
      language: "sql",
      explanation: "L'event log est la source principale pour surveiller un pipeline DLT. Les métriques de qualité sont dans les événements flow_progress, au format JSON dans la colonne details.",
    },
  },
  {
    id: "diagnostiquer-pipeline",
    title: "Diagnostiquer un pipeline",
    description: "Analysez un scénario d'échec de pipeline et expliquez comment utiliser l'event log pour trouver la cause racine.",
    difficulty: "difficile",
    type: "reflexion",
    prompt: "Votre pipeline DLT échoue lors de la mise à jour de la table Silver. Le DAG montre que la table Bronze est en succès mais Silver est en rouge. Expliquez la démarche complète pour diagnostiquer le problème en utilisant l'event log et l'interface DLT.",
    hints: [
      "Commencez par vérifier les événements d'erreur dans l'event log",
      "Vérifiez si une expectation FAIL UPDATE a déclenché l'arrêt",
      "Examinez les métriques flow_progress pour la table Silver",
    ],
    solution: {
      explanation: "Démarche de diagnostic : 1) Consulter le DAG pour identifier la table en échec (Silver). 2) Interroger l'event log avec WHERE level = 'ERROR' pour obtenir le message d'erreur exact. 3) Vérifier si l'échec est dû à une expectation FAIL UPDATE en filtrant sur event_type = 'flow_progress' et en examinant data_quality. 4) Si c'est une expectation, identifier les données invalides dans Bronze. 5) Si c'est une erreur de transformation, examiner le message d'erreur pour corriger la requête SQL/Python. 6) Utiliser Repair Run pour relancer uniquement la table Silver après correction.",
    },
  },
];

export default function ResultatsPipelinePage() {
  return (
    <div className="flex min-h-[calc(100vh-4rem)]">
      <Sidebar currentPath="/modules/4-2-resultats-pipeline" />

      <main className="flex-1 overflow-y-auto">
        <div className="max-w-4xl mx-auto px-6 py-10 lg:px-10">
          {/* Header */}
          <div className="mb-10">
            <div className="flex items-center gap-3 mb-3">
              <span className="inline-flex items-center px-3 py-1 rounded-full text-xs font-semibold bg-blue-100 text-blue-800">
                Module 4
              </span>
              <span className="text-sm text-[var(--color-text-light)]">
                Leçon 4.2
              </span>
            </div>
            <h1 className="text-3xl font-bold text-[var(--color-text)] mb-3">
              Résultats du Pipeline
            </h1>
            <p className="text-lg text-[var(--color-text-light)] leading-relaxed">
              Apprenez à explorer les résultats d&apos;un pipeline Delta Live
              Tables : structure de stockage, event log, métriques de qualité
              des données, et outils de surveillance pour garantir le bon
              fonctionnement de vos pipelines en production.
            </p>
          </div>

          {/* Content */}
          <section className="space-y-8">
            {/* Where are results stored */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Où sont stockés les résultats d&apos;un pipeline DLT ?
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Lorsque vous créez un pipeline DLT, Databricks gère
                automatiquement un emplacement de stockage (
                <strong>Storage Location</strong>) qui contient toutes les
                données produites par le pipeline, ainsi que les métadonnées
                nécessaires à son fonctionnement.
              </p>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Cet emplacement est configuré lors de la création du pipeline
                et contient plusieurs sous-répertoires organisés de manière
                structurée.
              </p>
            </div>

            {/* Storage Structure */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Structure du stockage DLT
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Le répertoire de stockage d&apos;un pipeline DLT est organisé
                comme suit :
              </p>
              <div className="bg-gray-50 border border-gray-200 rounded-lg p-5 font-mono text-sm text-[var(--color-text)] mb-4">
                <p className="mb-1">📂 pipeline_storage_location/</p>
                <p className="mb-1 ml-4">
                  📂 <strong>system/</strong>
                </p>
                <p className="mb-1 ml-8">📄 event_log (journal des événements)</p>
                <p className="mb-1 ml-8">📄 checkpoints (points de reprise streaming)</p>
                <p className="mb-1 ml-4">
                  📂 <strong>tables/</strong>
                </p>
                <p className="mb-1 ml-8">📄 table_1/ (tables Delta réelles)</p>
                <p className="mb-1 ml-8">📄 table_2/</p>
                <p className="mb-1 ml-4">
                  📂 <strong>autoloader/</strong>
                </p>
                <p className="ml-8">📄 schema/ (suivi des schémas Auto Loader)</p>
              </div>
              <ul className="list-disc list-inside space-y-3 text-[var(--color-text-light)] mb-4">
                <li>
                  <strong>system/</strong> : contient l&apos;event log
                  (journal des événements du pipeline) et les checkpoints
                  nécessaires au traitement incrémental.
                </li>
                <li>
                  <strong>tables/</strong> : contient les tables Delta
                  réelles produites par le pipeline. Chaque table est stockée
                  en tant que table Delta standard.
                </li>
                <li>
                  <strong>autoloader/</strong> : contient les métadonnées de
                  suivi des schémas utilisées par Auto Loader pour
                  l&apos;évolution automatique des schémas.
                </li>
              </ul>
            </div>

            {/* Event Log */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                L&apos;Event Log : journal du pipeline
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                L&apos;<strong>event log</strong> est la source principale
                d&apos;information pour surveiller et diagnostiquer un
                pipeline DLT. Il enregistre tous les événements du cycle de
                vie du pipeline : création de tables, progression des flux,
                résultats de qualité des données, et bien plus.
              </p>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Vous pouvez interroger l&apos;event log directement en SQL
                grâce à la fonction <code>event_log()</code> :
              </p>
              <CodeBlock
                language="sql"
                title="Interroger l'event log d'un pipeline"
                code={`-- Consulter tous les événements du pipeline
SELECT * FROM event_log("my_pipeline")

-- Filtrer par type d'événement
SELECT *
FROM event_log("my_pipeline")
WHERE event_type = 'flow_progress'
ORDER BY timestamp DESC

-- Voir les derniers événements
SELECT timestamp, event_type, message
FROM event_log("my_pipeline")
ORDER BY timestamp DESC
LIMIT 20`}
              />

              <InfoBox type="tip" title="event_log() : votre outil principal de surveillance">
                <p>
                  La fonction <code>event_log()</code> est le moyen principal
                  pour surveiller vos pipelines DLT. Elle vous permet de
                  consulter l&apos;historique complet des exécutions, de
                  diagnostiquer les erreurs, et de suivre les métriques de
                  qualité des données. Familiarisez-vous avec cette fonction,
                  elle est essentielle pour l&apos;examen de certification.
                </p>
              </InfoBox>
            </div>

            {/* Event Types */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Types d&apos;événements
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                L&apos;event log contient plusieurs types d&apos;événements,
                chacun fournissant des informations spécifiques :
              </p>
              <div className="overflow-x-auto mb-4">
                <table className="w-full border-collapse border border-gray-300 text-sm">
                  <thead>
                    <tr className="bg-gray-100">
                      <th className="border border-gray-300 px-4 py-2 text-left font-semibold text-[var(--color-text)]">
                        Type d&apos;événement
                      </th>
                      <th className="border border-gray-300 px-4 py-2 text-left font-semibold text-[var(--color-text)]">
                        Description
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr>
                      <td className="border border-gray-300 px-4 py-2 font-mono text-sm text-[var(--color-text-light)]">
                        flow_definition
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Définition des flux de données dans le pipeline (requêtes SQL/Python)
                      </td>
                    </tr>
                    <tr>
                      <td className="border border-gray-300 px-4 py-2 font-mono text-sm text-[var(--color-text-light)]">
                        flow_progress
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Progression des flux : nombre de lignes traitées, métriques de qualité
                      </td>
                    </tr>
                    <tr>
                      <td className="border border-gray-300 px-4 py-2 font-mono text-sm text-[var(--color-text-light)]">
                        dataset_definition
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Définition des jeux de données (tables, vues) créés par le pipeline
                      </td>
                    </tr>
                    <tr>
                      <td className="border border-gray-300 px-4 py-2 font-mono text-sm text-[var(--color-text-light)]">
                        maintenance
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Opérations de maintenance automatique (OPTIMIZE, VACUUM)
                      </td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </div>

            {/* DAG Visualization */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Visualisation du DAG du pipeline
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                DLT génère automatiquement un{" "}
                <strong>DAG (Directed Acyclic Graph)</strong> qui représente
                visuellement les dépendances entre toutes les tables du
                pipeline. Ce graphe montre :
              </p>
              <ul className="list-disc list-inside space-y-2 text-[var(--color-text-light)] mb-4">
                <li>Les sources de données externes</li>
                <li>Les tables intermédiaires et finales</li>
                <li>Les dépendances entre chaque table</li>
                <li>Le statut de chaque table (succès, échec, en cours)</li>
                <li>Les métriques de qualité (expectations) pour chaque table</li>
              </ul>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                La visualisation du DAG est accessible directement dans
                l&apos;interface Databricks lorsque vous consultez un
                pipeline DLT. C&apos;est un outil puissant pour comprendre la{" "}
                <strong>lignée des données (lineage)</strong> et diagnostiquer
                les problèmes.
              </p>
            </div>

            {/* Querying Data Quality */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Interroger les métriques de qualité des données
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Les résultats des expectations sont stockés dans l&apos;event
                log sous forme de données JSON imbriquées. Vous pouvez
                extraire ces métriques avec des requêtes SQL utilisant la
                notation semi-structurée (<code>:</code>) :
              </p>
              <CodeBlock
                language="sql"
                title="Extraire les métriques de qualité des données"
                code={`SELECT
  details:flow_progress.metrics.num_output_rows AS output_rows,
  details:flow_progress.data_quality.expectations
FROM event_log("my_pipeline")
WHERE event_type = 'flow_progress'`}
              />

              <CodeBlock
                language="sql"
                title="Analyse détaillée des résultats de qualité"
                code={`-- Extraire les détails des expectations
SELECT
  timestamp,
  details:flow_progress.metrics.num_output_rows AS lignes_produites,
  details:flow_progress.data_quality.expectations.name AS nom_contrainte,
  details:flow_progress.data_quality.expectations.dataset AS table_cible,
  details:flow_progress.data_quality.expectations.passed_records AS enregistrements_valides,
  details:flow_progress.data_quality.expectations.failed_records AS enregistrements_invalides
FROM event_log("my_pipeline")
WHERE event_type = 'flow_progress'
  AND details:flow_progress.data_quality IS NOT NULL
ORDER BY timestamp DESC`}
              />

              <InfoBox type="info" title="Format des métriques de qualité">
                <p>
                  Les métriques de qualité sont stockées au format JSON dans
                  la colonne <code>details</code> de l&apos;event log.
                  Utilisez la notation{" "}
                  <code>details:flow_progress.data_quality.expectations</code>{" "}
                  pour accéder aux résultats. Chaque expectation contient le
                  nombre d&apos;enregistrements validés (
                  <code>passed_records</code>) et rejetés (
                  <code>failed_records</code>).
                </p>
              </InfoBox>
            </div>

            {/* Pipeline Update Results */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Résultats et historique des mises à jour
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Chaque exécution d&apos;un pipeline DLT est appelée une{" "}
                <strong>mise à jour (update)</strong>. L&apos;historique de
                toutes les mises à jour est conservé et accessible dans
                l&apos;interface Databricks. Pour chaque mise à jour, vous
                pouvez consulter :
              </p>
              <ul className="list-disc list-inside space-y-2 text-[var(--color-text-light)] mb-4">
                <li>Le statut global (succès, échec, annulé)</li>
                <li>La durée d&apos;exécution</li>
                <li>Le nombre de lignes traitées par table</li>
                <li>Les résultats des expectations</li>
                <li>Les messages d&apos;erreur en cas d&apos;échec</li>
                <li>Le DAG avec le statut de chaque étape</li>
              </ul>

              <CodeBlock
                language="sql"
                title="Consulter l'historique des mises à jour"
                code={`-- Voir les mises à jour récentes du pipeline
SELECT
  id,
  timestamp,
  event_type,
  message
FROM event_log("my_pipeline")
WHERE event_type IN ('update_progress', 'create_update', 'update_completed')
ORDER BY timestamp DESC`}
              />
            </div>

            {/* Cluster Configuration */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Configuration du cluster DLT
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Contrairement aux clusters interactifs classiques, les
                clusters DLT sont{" "}
                <strong>entièrement gérés par Databricks</strong>. Vous
                n&apos;avez pas à créer ou configurer manuellement un cluster
                pour exécuter un pipeline DLT.
              </p>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                DLT gère automatiquement :
              </p>
              <ul className="list-disc list-inside space-y-2 text-[var(--color-text-light)] mb-4">
                <li>Le provisionnement et le démarrage du cluster</li>
                <li>Le dimensionnement automatique (autoscaling)</li>
                <li>L&apos;arrêt du cluster après l&apos;exécution (en mode Production)</li>
                <li>La configuration optimale pour les charges de travail DLT</li>
              </ul>

              <InfoBox type="important" title="DLT gère ses propres clusters">
                <p>
                  Vous ne pouvez pas utiliser un cluster interactif existant
                  pour exécuter un pipeline DLT. DLT provisionne et gère ses
                  propres clusters automatiquement. Vous pouvez cependant
                  configurer certains paramètres comme le type d&apos;instance,
                  le nombre minimum/maximum de workers, et les politiques de
                  cluster.
                </p>
              </InfoBox>
            </div>

            {/* Lineage */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Comprendre la lignée des données (Lineage)
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                La <strong>lignée des données (data lineage)</strong> décrit
                le parcours des données depuis leur source jusqu&apos;à leur
                destination finale. Dans DLT, la lignée est automatiquement
                tracée grâce aux références <code>LIVE.</code> entre les
                tables.
              </p>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Le DAG du pipeline est une représentation visuelle directe de
                cette lignée. Il vous permet de :
              </p>
              <ul className="list-disc list-inside space-y-2 text-[var(--color-text-light)] mb-4">
                <li>
                  <strong>Tracer l&apos;impact</strong> : identifier toutes
                  les tables affectées par un changement en amont
                </li>
                <li>
                  <strong>Diagnostiquer les erreurs</strong> : remonter la
                  chaîne de dépendances pour trouver la source d&apos;un
                  problème
                </li>
                <li>
                  <strong>Documenter le flux</strong> : comprendre comment les
                  données sont transformées à chaque étape
                </li>
              </ul>
            </div>
          </section>

          {/* Quiz */}
          <Quiz
            lessonSlug="4-2-resultats-pipeline"
            title="Quiz — Résultats du Pipeline"
            questions={quizQuestions}
          />

          {/* Exercices */}
          <LessonExercises
            lessonSlug="4-2-resultats-pipeline"
            exercises={exercises}
          />

          {/* Complétion */}
          <LessonCompleteButton lessonSlug="4-2-resultats-pipeline" />

          {/* Navigation */}
          <div className="flex flex-col sm:flex-row justify-between gap-4 mt-12 pt-8 border-t border-[var(--color-border)]">
            <Link
              href="/modules/4-1-delta-live-tables"
              className="inline-flex items-center gap-2 px-5 py-2.5 border border-gray-300 text-[var(--color-text)] rounded-lg font-medium hover:bg-gray-50 transition-colors"
            >
              <svg
                className="w-4 h-4"
                fill="none"
                stroke="currentColor"
                viewBox="0 0 24 24"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2}
                  d="M15 19l-7-7 7-7"
                />
              </svg>
              Leçon précédente : Delta Live Tables
            </Link>
            <Link
              href="/modules/4-3-orchestration-jobs"
              className="inline-flex items-center gap-2 px-5 py-2.5 bg-[#ff3621] text-white rounded-lg font-medium hover:bg-[#e02e1a] transition-colors"
            >
              Leçon suivante : Orchestration avec Jobs
              <svg
                className="w-4 h-4"
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
            </Link>
          </div>
        </div>
      </main>
    </div>
  );
}
