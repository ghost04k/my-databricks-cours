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
    question: "Quel format utilise Auto Loader pour lire les fichiers ?",
    options: [
      "autoLoader",
      "cloudFiles",
      "streamFiles",
      "deltaLoader"
    ],
    correctIndex: 1,
    explanation: "Auto Loader utilise le format 'cloudFiles' dans l'API Spark Structured Streaming. C'est le format spécifique à Databricks pour l'ingestion incrémentale de fichiers depuis le stockage cloud."
  },
  {
    question: "Quelle est la principale différence entre Auto Loader et COPY INTO ?",
    options: [
      "COPY INTO est plus rapide",
      "Auto Loader utilise le streaming et scale mieux pour des millions de fichiers",
      "Auto Loader ne supporte que JSON",
      "Pas de différence"
    ],
    correctIndex: 1,
    explanation: "Auto Loader utilise le streaming (Structured Streaming) et peut découvrir les nouveaux fichiers via des événements cloud (File Notification), ce qui le rend beaucoup plus performant que COPY INTO pour des millions de fichiers. COPY INTO doit lister le répertoire à chaque exécution."
  },
  {
    question: "À quoi sert la colonne _rescued_data ?",
    options: [
      "Sauvegarder les données supprimées",
      "Archiver les anciennes versions",
      "Stocker les données qui ne correspondent pas au schéma attendu",
      "Créer des backups"
    ],
    correctIndex: 2,
    explanation: "La colonne _rescued_data capture automatiquement les données qui ne correspondent pas au schéma attendu (colonnes inconnues, types incompatibles). Cela permet de ne perdre aucune donnée tout en maintenant un schéma propre pour les colonnes connues."
  },
  {
    question: "Pourquoi faut-il spécifier schemaLocation avec Auto Loader ?",
    options: [
      "C'est optionnel",
      "Pour accélérer le traitement",
      "Pour que Auto Loader puisse sauvegarder et faire évoluer le schéma inféré",
      "Pour compresser les fichiers"
    ],
    correctIndex: 2,
    explanation: "schemaLocation permet à Auto Loader de persister le schéma inféré et de détecter les évolutions de schéma au fil du temps (nouvelles colonnes, changements de types). Sans cela, le schéma serait ré-inféré à chaque redémarrage du stream."
  },
  {
    question: "Quel mode de découverte de fichiers scale le mieux ?",
    options: [
      "Directory Listing",
      "File Notification (via les événements cloud)",
      "Les deux sont identiques",
      "Manual Listing"
    ],
    correctIndex: 1,
    explanation: "Le mode File Notification s'appuie sur les événements du cloud (Azure Event Grid, AWS SNS/SQS) pour détecter les nouveaux fichiers, ce qui le rend beaucoup plus performant que Directory Listing qui doit scanner le répertoire entièrement à chaque itération."
  }
];

const exercises: LessonExercise[] = [
  {
    id: "configurer-auto-loader",
    title: "Configurer un Auto Loader",
    description: "Écrivez un pipeline Auto Loader complet pour ingérer des fichiers JSON avec inférence de schéma.",
    difficulty: "moyen",
    type: "code",
    prompt: "Créez un pipeline Auto Loader qui lit des fichiers JSON depuis '/mnt/data/raw/events/', active l'inférence de schéma et l'évolution, et écrit les résultats dans une table Delta 'bronze_events'.",
    hints: [
      "Le format à utiliser est 'cloudFiles' avec l'option 'cloudFiles.format' = 'json'",
      "Activez l'inférence avec 'cloudFiles.inferColumnTypes' = 'true'",
      "N'oubliez pas schemaLocation pour la persistance du schéma et checkpointLocation pour le stream"
    ],
    solution: {
      code: `# Pipeline Auto Loader complet\ndf_auto = (spark.readStream\n  .format("cloudFiles")\n  .option("cloudFiles.format", "json")\n  .option("cloudFiles.inferColumnTypes", "true")\n  .option("cloudFiles.schemaLocation", "/mnt/checkpoints/events_schema")\n  .option("cloudFiles.schemaEvolutionMode", "addNewColumns")\n  .load("/mnt/data/raw/events/")\n)\n\n# Écrire en Delta avec checkpoint\nquery = (df_auto.writeStream\n  .format("delta")\n  .outputMode("append")\n  .option("checkpointLocation", "/mnt/checkpoints/bronze_events")\n  .option("mergeSchema", "true")\n  .trigger(availableNow=True)\n  .toTable("bronze_events")\n)`,
      language: "python",
      explanation: "Ce pipeline utilise Auto Loader (cloudFiles) pour découvrir et lire incrémentalement les fichiers JSON. L'inférence de types et l'évolution de schéma sont activées pour gérer automatiquement les changements. Le résultat est écrit en Delta avec mergeSchema pour accepter les nouvelles colonnes."
    }
  },
  {
    id: "gerer-evolution-schema",
    title: "Gérer l'évolution de schéma",
    description: "Expliquez comment Auto Loader gère les différentes situations d'évolution de schéma.",
    difficulty: "difficile",
    type: "reflexion",
    prompt: "Décrivez comment Auto Loader gère les scénarios suivants : 1) Ajout de nouvelles colonnes dans les fichiers source, 2) Changement du type d'une colonne existante, 3) Données corrompues ou malformées. Quelles options de configuration utiliser ?",
    hints: [
      "Pensez aux différents modes de schemaEvolutionMode : addNewColumns, rescue, failOnNewColumns, none",
      "La colonne _rescued_data joue un rôle clé pour les données non conformes",
      "Considérez le mode rescue pour les changements de types incompatibles"
    ],
    solution: {
      code: `# 1. Nouvelles colonnes :\n# schemaEvolutionMode = \"addNewColumns\" (par défaut)\n# Auto Loader détecte les nouvelles colonnes et redémarre\n# le stream pour inclure les nouvelles colonnes.\n# Avec mergeSchema=true sur le writeStream, les colonnes\n# sont ajoutées à la table Delta.\n\n# 2. Changement de type :\n# Si un champ passe de STRING à INT (ou vice versa),\n# les données non conformes vont dans _rescued_data.\n# schemaEvolutionMode = \"rescue\" les capture sans erreur.\n# Pour forcer l'arrêt : \"failOnNewColumns\"\n\n# 3. Données corrompues :\n# Les fichiers malformés sont capturés dans _rescued_data\n# Option supplémentaire :\n# .option(\"badRecordsPath\", \"/mnt/quarantine/\")\n# pour isoler les fichiers problématiques\n\n# Configuration recommandée :\n# .option(\"cloudFiles.schemaEvolutionMode\", \"addNewColumns\")\n# .option(\"rescuedDataColumn\", \"_rescued_data\")\n# .option(\"cloudFiles.inferColumnTypes\", \"true\")`,
      language: "python",
      explanation: "Auto Loader gère l'évolution de schéma de manière robuste : les nouvelles colonnes sont automatiquement détectées et ajoutées, les données non conformes sont capturées dans _rescued_data, et les fichiers corrompus peuvent être isolés. Cette approche garantit qu'aucune donnée n'est perdue."
    }
  }
];

export default function AutoLoaderPage() {
  return (
    <div className="flex min-h-[calc(100vh-4rem)]">
      <Sidebar currentPath="/modules/3-2-auto-loader" />

      <main className="flex-1 overflow-y-auto">
        <div className="max-w-4xl mx-auto px-6 py-10 lg:px-10">
          {/* Header */}
          <div className="mb-10">
            <div className="flex items-center gap-3 mb-3">
              <span className="inline-flex items-center px-3 py-1 rounded-full text-xs font-semibold bg-blue-100 text-blue-800">
                Module 3
              </span>
              <span className="text-sm text-[var(--color-text-light)]">
                Leçon 3.2
              </span>
            </div>
            <h1 className="text-3xl font-bold text-[var(--color-text)] mb-3">
              Auto Loader
            </h1>
            <p className="text-lg text-[var(--color-text-light)] leading-relaxed">
              Maîtrisez Auto Loader (cloudFiles), la solution optimisée de
              Databricks pour l&apos;ingestion incrémentale de fichiers. Comparez-le
              avec COPY INTO et apprenez à gérer l&apos;évolution de schéma
              automatiquement.
            </p>
          </div>

          {/* Content */}
          <section className="space-y-8">
            {/* Introduction */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Qu&apos;est-ce qu&apos;Auto Loader ?
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                <strong>Auto Loader</strong> est une fonctionnalité de Databricks
                qui permet d&apos;ingérer de manière incrémentale et efficace de
                nouveaux fichiers de données à partir d&apos;un stockage cloud
                (Azure Blob Storage, AWS S3, Google Cloud Storage). Il utilise le
                format{" "}
                <code className="bg-gray-100 px-1.5 py-0.5 rounded text-sm font-mono">
                  cloudFiles
                </code>{" "}
                dans l&apos;API Spark Structured Streaming.
              </p>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Auto Loader est construit sur <strong>Structured Streaming</strong>{" "}
                et hérite donc de tous ses avantages : traitement incrémental,
                checkpointing, tolérance aux pannes et garantie exactly-once.
              </p>
            </div>

            {/* Auto Loader vs COPY INTO */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Auto Loader vs COPY INTO
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Databricks propose deux mécanismes pour ingérer des fichiers :{" "}
                <strong>Auto Loader</strong> et{" "}
                <strong>COPY INTO</strong>. Voici leurs différences :
              </p>
              <div className="overflow-x-auto my-6">
                <table className="w-full border-collapse border border-gray-200 text-sm">
                  <thead>
                    <tr className="bg-gray-50">
                      <th className="border border-gray-200 px-4 py-3 text-left font-semibold text-[var(--color-text)]">
                        Critère
                      </th>
                      <th className="border border-gray-200 px-4 py-3 text-left font-semibold text-[var(--color-text)]">
                        Auto Loader
                      </th>
                      <th className="border border-gray-200 px-4 py-3 text-left font-semibold text-[var(--color-text)]">
                        COPY INTO
                      </th>
                    </tr>
                  </thead>
                  <tbody className="text-[var(--color-text-light)]">
                    <tr>
                      <td className="border border-gray-200 px-4 py-3 font-medium">
                        Scalabilité
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Excellent — des millions de fichiers
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Limité — des milliers de fichiers
                      </td>
                    </tr>
                    <tr className="bg-gray-50">
                      <td className="border border-gray-200 px-4 py-3 font-medium">
                        Suivi des fichiers
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Via checkpoints (RocksDB)
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Via historique de la table
                      </td>
                    </tr>
                    <tr>
                      <td className="border border-gray-200 px-4 py-3 font-medium">
                        Évolution de schéma
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Automatique (inférence + évolution)
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Manuelle
                      </td>
                    </tr>
                    <tr className="bg-gray-50">
                      <td className="border border-gray-200 px-4 py-3 font-medium">
                        Moteur sous-jacent
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Structured Streaming
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Commande SQL (batch)
                      </td>
                    </tr>
                    <tr>
                      <td className="border border-gray-200 px-4 py-3 font-medium">
                        Cas d&apos;usage recommandé
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Ingestion à grande échelle, production
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Petits volumes, chargements ponctuels
                      </td>
                    </tr>
                  </tbody>
                </table>
              </div>
              <InfoBox type="tip" title="Auto Loader est le choix recommandé">
                <p>
                  Pour l&apos;ingestion de fichiers à grande échelle en production,
                  Databricks recommande <strong>Auto Loader</strong> plutôt que
                  COPY INTO. Auto Loader est plus performant, plus scalable et
                  gère automatiquement l&apos;évolution du schéma. Retenez cela
                  pour l&apos;examen !
                </p>
              </InfoBox>
            </div>

            {/* File discovery modes */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Modes de découverte de fichiers
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Auto Loader propose deux modes pour détecter les nouveaux fichiers :
              </p>
              <ul className="list-disc list-inside space-y-3 text-[var(--color-text-light)] mb-4">
                <li>
                  <strong>Directory Listing</strong> (par défaut) : Auto Loader
                  liste régulièrement le contenu du répertoire pour identifier
                  les nouveaux fichiers. Simple à configurer, adapté pour la
                  plupart des cas d&apos;usage.
                </li>
                <li>
                  <strong>File Notification</strong> : Auto Loader s&apos;appuie
                  sur des services de notification du cloud (AWS SNS/SQS, Azure
                  Event Grid, GCP Pub/Sub) pour être informé des nouveaux
                  fichiers. Plus efficace pour les répertoires contenant un très
                  grand nombre de fichiers.
                </li>
              </ul>
              <CodeBlock
                language="python"
                title="Configuration du mode de découverte"
                code={`# Mode Directory Listing (par défaut)
(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.useNotifications", "false")  # Directory listing
    .load("/path/to/files")
)

# Mode File Notification
(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.useNotifications", "true")   # File notification
    .load("/path/to/files")
)`}
              />
            </div>

            {/* Schema inference and evolution */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Inférence et évolution du schéma
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                L&apos;un des atouts majeurs d&apos;Auto Loader est sa capacité
                à <strong>inférer automatiquement le schéma</strong> des données
                et à <strong>gérer son évolution</strong> dans le temps. Quand de
                nouvelles colonnes apparaissent dans les fichiers sources, Auto
                Loader peut les détecter et mettre à jour le schéma de la table
                cible.
              </p>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Pour activer l&apos;inférence de schéma, il faut spécifier un{" "}
                <strong>schemaLocation</strong> — un répertoire où Auto Loader
                stocke et met à jour le schéma inféré.
              </p>
              <InfoBox type="important" title="schemaLocation obligatoire">
                <p>
                  L&apos;option{" "}
                  <code className="bg-gray-100 px-1.5 py-0.5 rounded text-sm font-mono">
                    cloudFiles.schemaLocation
                  </code>{" "}
                  est <strong>obligatoire</strong> lorsqu&apos;on utilise
                  l&apos;inférence de schéma avec Auto Loader. Sans cette
                  option, Auto Loader ne pourra pas persister le schéma inféré
                  entre les exécutions.
                </p>
              </InfoBox>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4 mt-4">
                Les options d&apos;évolution du schéma disponibles sont :
              </p>
              <ul className="list-disc list-inside space-y-2 text-[var(--color-text-light)] mb-4">
                <li>
                  <strong>addNewColumns</strong> (par défaut) : Les nouvelles
                  colonnes sont automatiquement ajoutées au schéma.
                </li>
                <li>
                  <strong>failOnNewColumns</strong> : Le flux échoue si de
                  nouvelles colonnes sont détectées, ce qui permet une
                  intervention manuelle.
                </li>
                <li>
                  <strong>rescue</strong> : Les colonnes inconnues sont
                  capturées dans une colonne spéciale{" "}
                  <code className="bg-gray-100 px-1.5 py-0.5 rounded text-sm font-mono">
                    _rescued_data
                  </code>
                  .
                </li>
                <li>
                  <strong>none</strong> : Aucune évolution de schéma. Les
                  nouvelles colonnes sont ignorées.
                </li>
              </ul>
            </div>

            {/* _rescued_data */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                La colonne _rescued_data
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Lorsque Auto Loader rencontre des données qui ne correspondent
                pas au schéma attendu (mauvais type, colonne inconnue, données
                corrompues), il peut les stocker dans une colonne spéciale
                appelée{" "}
                <code className="bg-gray-100 px-1.5 py-0.5 rounded text-sm font-mono">
                  _rescued_data
                </code>
                . Cette colonne contient les données problématiques au format JSON.
              </p>
              <InfoBox type="info" title="Colonne _rescued_data">
                <p>
                  La colonne{" "}
                  <code className="bg-gray-100 px-1.5 py-0.5 rounded text-sm font-mono">
                    _rescued_data
                  </code>{" "}
                  est automatiquement ajoutée par Auto Loader lorsque des
                  données ne correspondent pas au schéma. Elle permet de ne
                  perdre aucune donnée tout en isolant les enregistrements
                  problématiques pour un traitement ultérieur.
                </p>
              </InfoBox>
            </div>

            {/* Code examples Python */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Utilisation en Python (PySpark)
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Voici un exemple complet d&apos;ingestion de fichiers JSON avec
                Auto Loader, incluant l&apos;inférence de schéma et
                l&apos;évolution automatique :
              </p>
              <CodeBlock
                language="python"
                title="Auto Loader — Ingestion de fichiers JSON"
                code={`# Lecture de fichiers JSON avec Auto Loader
(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/path/to/schema")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load("/path/to/json/files")
    .writeStream
    .option("checkpointLocation", "/path/to/checkpoint")
    .option("mergeSchema", "true")
    .table("target_table")
)

# Avec des schemaHints pour guider l'inférence
(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/path/to/schema")
    .option("cloudFiles.schemaHints", "id INT, price DOUBLE, name STRING")
    .load("/path/to/json/files")
    .writeStream
    .option("checkpointLocation", "/path/to/checkpoint")
    .table("target_table_with_hints")
)`}
              />
              <CodeBlock
                language="python"
                title="Auto Loader — Ingestion de fichiers CSV"
                code={`# Lecture de fichiers CSV avec Auto Loader
(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", "/path/to/schema_csv")
    .option("header", "true")
    .option("sep", ",")
    .load("/path/to/csv/files")
    .writeStream
    .option("checkpointLocation", "/path/to/checkpoint_csv")
    .table("target_csv_table")
)`}
              />
            </div>

            {/* SQL syntax */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Utilisation en SQL
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Auto Loader peut également être utilisé en SQL via la fonction{" "}
                <code className="bg-gray-100 px-1.5 py-0.5 rounded text-sm font-mono">
                  cloud_files()
                </code>
                , principalement dans le contexte de{" "}
                <strong>Delta Live Tables</strong> :
              </p>
              <CodeBlock
                language="sql"
                title="Auto Loader en SQL (Delta Live Tables)"
                code={`-- Créer une table streaming avec Auto Loader en SQL
CREATE OR REFRESH STREAMING TABLE my_table
AS SELECT * FROM cloud_files(
  "/path/to/files",
  "json",
  map(
    "cloudFiles.schemaHints", "col1 INT, col2 STRING",
    "cloudFiles.schemaEvolutionMode", "addNewColumns"
  )
);

-- Avec un format CSV
CREATE OR REFRESH STREAMING TABLE csv_table
AS SELECT * FROM cloud_files(
  "/path/to/csv/files",
  "csv",
  map(
    "header", "true",
    "delimiter", ","
  )
);`}
              />
            </div>

            {/* Récapitulatif */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Récapitulatif
              </h2>
              <div className="overflow-x-auto my-6">
                <table className="w-full border-collapse border border-gray-200 text-sm">
                  <thead>
                    <tr className="bg-gray-50">
                      <th className="border border-gray-200 px-4 py-3 text-left font-semibold text-[var(--color-text)]">
                        Fonctionnalité
                      </th>
                      <th className="border border-gray-200 px-4 py-3 text-left font-semibold text-[var(--color-text)]">
                        Description
                      </th>
                    </tr>
                  </thead>
                  <tbody className="text-[var(--color-text-light)]">
                    <tr>
                      <td className="border border-gray-200 px-4 py-3 font-medium">
                        cloudFiles
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Format de source streaming pour Auto Loader
                      </td>
                    </tr>
                    <tr className="bg-gray-50">
                      <td className="border border-gray-200 px-4 py-3 font-medium">
                        schemaLocation
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Répertoire de stockage du schéma inféré (obligatoire)
                      </td>
                    </tr>
                    <tr>
                      <td className="border border-gray-200 px-4 py-3 font-medium">
                        schemaHints
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Indications de types pour guider l&apos;inférence
                      </td>
                    </tr>
                    <tr className="bg-gray-50">
                      <td className="border border-gray-200 px-4 py-3 font-medium">
                        schemaEvolutionMode
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Comportement face aux nouvelles colonnes (addNewColumns, failOnNewColumns, rescue, none)
                      </td>
                    </tr>
                    <tr>
                      <td className="border border-gray-200 px-4 py-3 font-medium">
                        _rescued_data
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Colonne pour les données ne correspondant pas au schéma
                      </td>
                    </tr>
                    <tr className="bg-gray-50">
                      <td className="border border-gray-200 px-4 py-3 font-medium">
                        cloud_files()
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Fonction SQL pour utiliser Auto Loader dans DLT
                      </td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </div>
          </section>

          {/* Quiz */}
          <Quiz
            lessonSlug="3-2-auto-loader"
            title="Quiz — Auto Loader"
            questions={quizQuestions}
          />

          {/* Exercices */}
          <LessonExercises
            lessonSlug="3-2-auto-loader"
            exercises={exercises}
          />

          {/* Bouton de complétion */}
          <LessonCompleteButton lessonSlug="3-2-auto-loader" />

          {/* Navigation */}
          <div className="flex flex-col sm:flex-row justify-between gap-4 mt-12 pt-8 border-t border-[var(--color-border)]">
            <Link
              href="/modules/3-1-structured-streaming"
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
              Leçon précédente : Structured Streaming
            </Link>
            <Link
              href="/modules/3-3-architecture-multi-hop"
              className="inline-flex items-center gap-2 px-5 py-2.5 bg-[#ff3621] text-white rounded-lg font-medium hover:bg-[#e02e1a] transition-colors"
            >
              Leçon suivante : Architecture Multi-Hop
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
