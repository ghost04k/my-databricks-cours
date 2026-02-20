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
    question: "Quel est le principal avantage de DLT par rapport à Spark traditionnel ?",
    options: ["Plus rapide", "Approche déclarative avec moins de code et gestion automatique des dépendances", "Supporte plus de langages", "Gratuit"],
    correctIndex: 1,
    explanation: "DLT permet de décrire QUOI faire (déclaratif) plutôt que COMMENT le faire, réduisant le boilerplate et les erreurs.",
  },
  {
    question: "Quel préfixe faut-il utiliser pour référencer une autre table DLT ?",
    options: ["dlt.", "LIVE.", "delta.", "stream."],
    correctIndex: 1,
    explanation: "Le préfixe LIVE. est obligatoire pour référencer d'autres tables au sein d'un pipeline DLT.",
  },
  {
    question: "Que fait l'expectation ON VIOLATION DROP ROW ?",
    options: ["Arrête le pipeline", "Supprime les lignes qui ne satisfont pas la contrainte mais continue", "Ignore la contrainte", "Log un avertissement"],
    correctIndex: 1,
    explanation: "DROP ROW supprime silencieusement les lignes invalides. FAIL UPDATE arrête le pipeline. Sans action, les lignes sont gardées mais métrique enregistrée.",
  },
  {
    question: "Quelle est la différence entre un Streaming Live Table et un Live Table ?",
    options: ["Streaming Live Table traite les données de manière incrémentale, Live Table recompute tout à chaque exécution", "Pas de différence", "Live Table est plus rapide", "Streaming Live Table ne supporte que JSON"],
    correctIndex: 0,
    explanation: "Streaming Live Table lit les données de manière incrémentale (append-only), tandis qu'un Live Table est recalculé entièrement à chaque mise à jour.",
  },
  {
    question: "En DLT Python, quel décorateur définit une contrainte de qualité ?",
    options: ["@dlt.constraint", "@dlt.expect", "@dlt.validate", "@dlt.check"],
    correctIndex: 1,
    explanation: "@dlt.expect() définit une contrainte de qualité. Variantes : @dlt.expect_or_drop() et @dlt.expect_or_fail().",
  },
];

const exercises: LessonExercise[] = [
  {
    id: "ecrire-pipeline-dlt",
    title: "Écrire un pipeline DLT",
    description: "Créez un pipeline DLT complet en SQL avec les trois couches Bronze, Silver et Gold.",
    difficulty: "moyen",
    type: "code",
    prompt: "Écrivez un pipeline DLT en SQL comprenant : une couche Bronze avec Auto Loader pour ingérer des fichiers JSON, une couche Silver avec nettoyage et expectations de qualité, et une couche Gold avec agrégation.",
    hints: [
      "Utilisez CREATE OR REFRESH STREAMING TABLE pour Bronze",
      "Ajoutez des CONSTRAINT EXPECT sur Silver",
      "Utilisez LIVE. pour référencer les tables",
    ],
    solution: {
      code: `-- Couche Bronze : ingestion avec Auto Loader\nCREATE OR REFRESH STREAMING TABLE orders_bronze\nCOMMENT "Données brutes ingérées en streaming"\nAS SELECT * FROM cloud_files(\n  "/mnt/data/orders",\n  "json",\n  map("cloudFiles.inferColumnTypes", "true")\n);\n\n-- Couche Silver : nettoyage + expectations\nCREATE OR REFRESH STREAMING TABLE orders_silver (\n  CONSTRAINT valid_order_id EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW,\n  CONSTRAINT valid_amount EXPECT (amount > 0) ON VIOLATION DROP ROW\n)\nCOMMENT "Commandes nettoyées et validées"\nAS SELECT\n  order_id,\n  customer_id,\n  CAST(amount AS DOUBLE) AS amount,\n  CAST(order_date AS DATE) AS order_date\nFROM STREAMING(LIVE.orders_bronze);\n\n-- Couche Gold : agrégation\nCREATE OR REFRESH LIVE TABLE orders_gold\nCOMMENT "Résumé des commandes par client"\nAS SELECT\n  customer_id,\n  COUNT(*) AS total_orders,\n  SUM(amount) AS total_amount\nFROM LIVE.orders_silver\nGROUP BY customer_id;`,
      language: "sql",
      explanation: "Le pipeline suit l'architecture Medallion : Bronze ingère les données brutes en streaming, Silver les nettoie avec des expectations DROP ROW pour filtrer les lignes invalides, et Gold agrège les résultats par client.",
    },
  },
  {
    id: "comparer-expectations",
    title: "Comparer les expectations",
    description: "Comprenez les trois types d'expectations et quand utiliser chacun.",
    difficulty: "facile",
    type: "reflexion",
    prompt: "Créez une table DLT avec des expectations utilisant chacun des trois types (WARN, DROP ROW, FAIL UPDATE) et expliquez dans quel cas utiliser chaque type.",
    hints: [
      "warn = on veut garder toutes les données mais monitoring",
      "drop = on filtre les données invalides",
      "fail = les données invalides sont inacceptables",
    ],
    solution: {
      explanation: "WARN (défaut) : conserve les lignes invalides mais enregistre une métrique. Utile pour le monitoring sans impact sur les données. DROP ROW : supprime les lignes invalides silencieusement. Idéal pour filtrer les données de mauvaise qualité (ex: NULL dans un ID). FAIL UPDATE : arrête le pipeline immédiatement. Utilisé quand les données invalides sont inacceptables et nécessitent une intervention humaine (ex: montant négatif dans une transaction financière).",
    },
  },
];

export default function DeltaLiveTablesPage() {
  return (
    <div className="flex min-h-[calc(100vh-4rem)]">
      <Sidebar currentPath="/modules/4-1-delta-live-tables" />

      <main className="flex-1 overflow-y-auto">
        <div className="max-w-4xl mx-auto px-6 py-10 lg:px-10">
          {/* Header */}
          <div className="mb-10">
            <div className="flex items-center gap-3 mb-3">
              <span className="inline-flex items-center px-3 py-1 rounded-full text-xs font-semibold bg-blue-100 text-blue-800">
                Module 4
              </span>
              <span className="text-sm text-[var(--color-text-light)]">
                Leçon 4.1
              </span>
            </div>
            <h1 className="text-3xl font-bold text-[var(--color-text)] mb-3">
              Delta Live Tables (DLT)
            </h1>
            <p className="text-lg text-[var(--color-text-light)] leading-relaxed">
              Découvrez Delta Live Tables, le framework déclaratif de
              Databricks pour construire des pipelines de données fiables et
              maintenables. Apprenez à définir des tables, gérer la qualité
              des données avec les expectations, et choisir le bon mode
              d&apos;exécution.
            </p>
          </div>

          {/* Content */}
          <section className="space-y-8">
            {/* Introduction */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Qu&apos;est-ce que Delta Live Tables (DLT) ?
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                <strong>Delta Live Tables (DLT)</strong> est un framework
                déclaratif proposé par Databricks pour simplifier la
                construction de pipelines de données. Au lieu d&apos;écrire
                du code impératif complexe pour gérer l&apos;ingestion, les
                transformations et la gestion des erreurs, DLT vous permet de{" "}
                <strong>déclarer ce que vous voulez obtenir</strong> et laisse
                le framework gérer le reste.
              </p>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                DLT prend en charge automatiquement la gestion des
                dépendances entre tables, l&apos;orchestration des
                transformations, les mécanismes de reprise sur erreur, et la
                surveillance de la qualité des données.
              </p>
            </div>

            {/* DLT vs Traditional */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                DLT vs approche Spark traditionnelle
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Avec l&apos;approche traditionnelle en Spark, vous devez gérer
                manuellement chaque étape : lire les données, appliquer les
                transformations, écrire les résultats, gérer les checkpoints,
                et orchestrer les dépendances entre les notebooks. Avec DLT,
                tout cela est simplifié :
              </p>
              <div className="overflow-x-auto mb-4">
                <table className="w-full border-collapse border border-gray-300 text-sm">
                  <thead>
                    <tr className="bg-gray-100">
                      <th className="border border-gray-300 px-4 py-2 text-left font-semibold text-[var(--color-text)]">
                        Aspect
                      </th>
                      <th className="border border-gray-300 px-4 py-2 text-left font-semibold text-[var(--color-text)]">
                        Spark traditionnel
                      </th>
                      <th className="border border-gray-300 px-4 py-2 text-left font-semibold text-[var(--color-text)]">
                        Delta Live Tables
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Approche
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Impérative (étape par étape)
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Déclarative (résultat souhaité)
                      </td>
                    </tr>
                    <tr>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Gestion des dépendances
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Manuelle
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Automatique
                      </td>
                    </tr>
                    <tr>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Qualité des données
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Code personnalisé
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Expectations intégrées
                      </td>
                    </tr>
                    <tr>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Gestion des erreurs
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Try/catch personnalisé
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Automatique avec retry
                      </td>
                    </tr>
                    <tr>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Infrastructure
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Cluster à gérer
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Cluster auto-géré
                      </td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </div>

            {/* Key Concepts */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Concepts clés de DLT
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                DLT repose sur trois concepts fondamentaux :
              </p>
              <ul className="list-disc list-inside space-y-3 text-[var(--color-text-light)] mb-4">
                <li>
                  <strong>Live Tables</strong> : tables matérialisées dont le
                  contenu est recalculé entièrement à chaque exécution.
                  Idéales pour les agrégations ou les transformations
                  complètes.
                </li>
                <li>
                  <strong>Streaming Live Tables</strong> : tables qui
                  traitent les données de manière incrémentale (streaming).
                  Seules les nouvelles données sont traitées à chaque
                  exécution.
                </li>
                <li>
                  <strong>Expectations</strong> : contraintes de qualité des
                  données qui permettent de valider, filtrer ou rejeter les
                  enregistrements selon des règles définies.
                </li>
              </ul>
            </div>

            {/* SQL Syntax */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Syntaxe SQL pour créer des tables DLT
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                DLT utilise une syntaxe SQL enrichie avec des mots-clés
                spécifiques. Voici les deux types principaux de tables :
              </p>

              <h3 className="text-xl font-semibold text-[var(--color-text)] mb-3">
                Streaming Live Table (données incrémentales)
              </h3>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-3">
                Utilisée pour ingérer des données de manière incrémentale,
                par exemple depuis un stockage cloud avec Auto Loader :
              </p>
              <CodeBlock
                language="sql"
                title="Streaming Live Table - Ingestion incrémentale"
                code={`-- Streaming Live Table (pour données incrémentales/streaming)
CREATE OR REFRESH STREAMING TABLE table_name
AS SELECT * FROM cloud_files("/path/to/data", "json")

-- Avec des options supplémentaires
CREATE OR REFRESH STREAMING TABLE orders_raw
COMMENT "Données brutes des commandes ingérées en streaming"
AS SELECT * FROM cloud_files(
  "/mnt/data/orders",
  "json",
  map("cloudFiles.inferColumnTypes", "true")
)`}
              />

              <h3 className="text-xl font-semibold text-[var(--color-text)] mt-6 mb-3">
                Live Table (recalcul complet)
              </h3>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-3">
                Utilisée pour les tables qui doivent être entièrement
                recalculées à chaque mise à jour, comme une vue matérialisée :
              </p>
              <CodeBlock
                language="sql"
                title="Live Table - Vue matérialisée"
                code={`-- Live Table (recalcul complet / vue matérialisée)
CREATE OR REFRESH LIVE TABLE table_name
AS SELECT * FROM LIVE.source_table

-- Exemple concret : table Silver nettoyée
CREATE OR REFRESH LIVE TABLE orders_clean
COMMENT "Commandes nettoyées et validées"
AS SELECT
  order_id,
  customer_id,
  CAST(order_date AS DATE) AS order_date,
  amount
FROM LIVE.orders_raw
WHERE order_id IS NOT NULL`}
              />

              <InfoBox type="important" title="Le préfixe LIVE. est obligatoire">
                <p>
                  Lorsque vous référencez une autre table DLT dans votre
                  pipeline, vous <strong>devez</strong> utiliser le préfixe{" "}
                  <strong>LIVE.</strong> devant le nom de la table source.
                  Par exemple : <code>LIVE.orders_raw</code>. Sans ce
                  préfixe, DLT ne reconnaîtra pas la dépendance entre les
                  tables et le pipeline échouera.
                </p>
              </InfoBox>
            </div>

            {/* Expectations */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Contraintes de qualité : Expectations
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Les <strong>Expectations</strong> sont le mécanisme de
                validation de la qualité des données dans DLT. Elles
                permettent de définir des règles que les données doivent
                respecter, avec trois actions possibles en cas de violation :
              </p>

              <CodeBlock
                language="sql"
                title="Les trois types d'actions pour les Expectations"
                code={`-- 1. WARN : les enregistrements invalides sont conservés mais signalés
-- (action par défaut si aucune action n'est spécifiée)
CONSTRAINT valid_id EXPECT (id IS NOT NULL)

-- 2. DROP ROW : les enregistrements invalides sont supprimés silencieusement
CONSTRAINT valid_id EXPECT (id IS NOT NULL) ON VIOLATION DROP ROW

-- 3. FAIL UPDATE : le pipeline échoue si des enregistrements sont invalides
CONSTRAINT valid_id EXPECT (id IS NOT NULL) ON VIOLATION FAIL UPDATE`}
              />

              <div className="overflow-x-auto my-6">
                <table className="w-full border-collapse border border-gray-300 text-sm">
                  <thead>
                    <tr className="bg-gray-100">
                      <th className="border border-gray-300 px-4 py-2 text-left font-semibold text-[var(--color-text)]">
                        Action
                      </th>
                      <th className="border border-gray-300 px-4 py-2 text-left font-semibold text-[var(--color-text)]">
                        Comportement
                      </th>
                      <th className="border border-gray-300 px-4 py-2 text-left font-semibold text-[var(--color-text)]">
                        Enregistrement invalide
                      </th>
                      <th className="border border-gray-300 px-4 py-2 text-left font-semibold text-[var(--color-text)]">
                        Pipeline
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr>
                      <td className="border border-gray-300 px-4 py-2 font-mono text-sm text-[var(--color-text-light)]">
                        WARN (défaut)
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Alerte enregistrée
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Conservé dans la table
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Continue normalement
                      </td>
                    </tr>
                    <tr>
                      <td className="border border-gray-300 px-4 py-2 font-mono text-sm text-[var(--color-text-light)]">
                        DROP ROW
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Suppression silencieuse
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Supprimé de la table
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Continue normalement
                      </td>
                    </tr>
                    <tr>
                      <td className="border border-gray-300 px-4 py-2 font-mono text-sm text-[var(--color-text-light)]">
                        FAIL UPDATE
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Échec immédiat
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Non écrit
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Échoue et s&apos;arrête
                      </td>
                    </tr>
                  </tbody>
                </table>
              </div>

              <h3 className="text-xl font-semibold text-[var(--color-text)] mb-3">
                Exemple complet avec Expectations en SQL
              </h3>
              <CodeBlock
                language="sql"
                title="Table DLT avec plusieurs contraintes de qualité"
                code={`CREATE OR REFRESH LIVE TABLE orders_validated (
  CONSTRAINT valid_order_id EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_amount EXPECT (amount > 0) ON VIOLATION DROP ROW,
  CONSTRAINT valid_date EXPECT (order_date > '2020-01-01') ON VIOLATION FAIL UPDATE
)
COMMENT "Commandes validées avec contraintes de qualité"
AS SELECT *
FROM LIVE.orders_clean`}
              />
            </div>

            {/* Python DLT Syntax */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Syntaxe Python pour DLT
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                DLT peut également être utilisé avec Python grâce au module{" "}
                <code>dlt</code>. Les décorateurs Python permettent de
                définir les tables et les contraintes de qualité :
              </p>
              <CodeBlock
                language="python"
                title="Définition d'une table DLT en Python avec Expectations"
                code={`import dlt

@dlt.table(
  comment="Description de la table"
)
@dlt.expect("valid_id", "id IS NOT NULL")
@dlt.expect_or_drop("valid_date", "date > '2020-01-01'")
@dlt.expect_or_fail("valid_amount", "amount > 0")
def my_table():
    return spark.readStream.table("LIVE.source_table")`}
              />

              <p className="text-[var(--color-text-light)] leading-relaxed my-4">
                Les trois décorateurs d&apos;expectations en Python
                correspondent aux actions SQL :
              </p>
              <ul className="list-disc list-inside space-y-2 text-[var(--color-text-light)] mb-4">
                <li>
                  <code>@dlt.expect()</code> → équivalent de{" "}
                  <code>EXPECT</code> (WARN, comportement par défaut)
                </li>
                <li>
                  <code>@dlt.expect_or_drop()</code> → équivalent de{" "}
                  <code>ON VIOLATION DROP ROW</code>
                </li>
                <li>
                  <code>@dlt.expect_or_fail()</code> → équivalent de{" "}
                  <code>ON VIOLATION FAIL UPDATE</code>
                </li>
              </ul>

              <h3 className="text-xl font-semibold text-[var(--color-text)] mb-3">
                Exemple complet d&apos;un pipeline Python DLT
              </h3>
              <CodeBlock
                language="python"
                title="Pipeline multi-couches complet en Python"
                code={`import dlt
from pyspark.sql.functions import col, current_timestamp

# Couche Bronze : ingestion streaming
@dlt.table(
  comment="Données brutes ingérées depuis le stockage cloud"
)
def orders_bronze():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("/mnt/data/orders")
    )

# Couche Silver : nettoyage et validation
@dlt.table(
  comment="Commandes nettoyées et validées"
)
@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("valid_amount", "amount > 0")
def orders_silver():
    return (
        dlt.read_stream("orders_bronze")
        .select(
            col("order_id"),
            col("customer_id"),
            col("amount").cast("double"),
            col("order_date").cast("date"),
            current_timestamp().alias("processed_at")
        )
    )

# Couche Gold : agrégation métier
@dlt.table(
  comment="Résumé des commandes par client"
)
def orders_gold():
    return (
        dlt.read("orders_silver")
        .groupBy("customer_id")
        .agg(
            {"amount": "sum", "order_id": "count"}
        )
    )`}
              />
            </div>

            {/* LIVE. prefix and STREAMING() */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Référencer les sources : LIVE. et STREAMING()
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Dans un pipeline DLT, deux mots-clés spéciaux sont utilisés
                pour référencer les sources de données :
              </p>
              <ul className="list-disc list-inside space-y-3 text-[var(--color-text-light)] mb-4">
                <li>
                  <strong>LIVE.</strong> : préfixe utilisé en SQL pour
                  référencer une autre table du même pipeline DLT. Par
                  exemple : <code>SELECT * FROM LIVE.orders_raw</code>.
                </li>
                <li>
                  <strong>STREAMING()</strong> : fonction SQL utilisée dans
                  une Streaming Live Table pour lire une source en streaming.
                  Par exemple :{" "}
                  <code>SELECT * FROM STREAMING(LIVE.orders_raw)</code>.
                </li>
              </ul>

              <CodeBlock
                language="sql"
                title="Utilisation de LIVE. et STREAMING()"
                code={`-- Live Table qui lit depuis une autre table DLT (recalcul complet)
CREATE OR REFRESH LIVE TABLE orders_summary
AS SELECT customer_id, SUM(amount) AS total
FROM LIVE.orders_clean
GROUP BY customer_id

-- Streaming Live Table qui lit en streaming depuis une autre table DLT
CREATE OR REFRESH STREAMING TABLE orders_enriched
AS SELECT *
FROM STREAMING(LIVE.orders_raw)
WHERE amount > 0`}
              />
            </div>

            {/* Pipeline Modes */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Modes d&apos;exécution du pipeline
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Un pipeline DLT peut être exécuté dans deux modes :
              </p>

              <h3 className="text-xl font-semibold text-[var(--color-text)] mb-3">
                Mode Triggered (déclenché)
              </h3>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Le pipeline traite toutes les données disponibles puis{" "}
                <strong>s&apos;arrête</strong>. C&apos;est le mode le plus
                courant pour les scénarios batch. Le cluster est libéré après
                l&apos;exécution, ce qui réduit les coûts.
              </p>

              <h3 className="text-xl font-semibold text-[var(--color-text)] mb-3">
                Mode Continuous (continu)
              </h3>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Le pipeline <strong>reste actif</strong> et traite les
                nouvelles données dès qu&apos;elles arrivent, avec une
                latence très faible. Le cluster reste en fonctionnement en
                permanence, ce qui entraîne des coûts plus élevés.
              </p>

              <InfoBox type="tip" title="Privilégiez le mode Triggered">
                <p>
                  Pour la majorité des scénarios batch et les cas
                  d&apos;utilisation courants, le mode{" "}
                  <strong>Triggered</strong> est recommandé. Il offre un bon
                  équilibre entre fraîcheur des données et maîtrise des
                  coûts. Réservez le mode Continuous aux cas nécessitant un
                  traitement quasi temps réel.
                </p>
              </InfoBox>
            </div>

            {/* Dev vs Prod */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Mode Development vs Production
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                DLT propose deux modes de fonctionnement pour la mise au
                point et le déploiement :
              </p>
              <div className="overflow-x-auto mb-4">
                <table className="w-full border-collapse border border-gray-300 text-sm">
                  <thead>
                    <tr className="bg-gray-100">
                      <th className="border border-gray-300 px-4 py-2 text-left font-semibold text-[var(--color-text)]">
                        Caractéristique
                      </th>
                      <th className="border border-gray-300 px-4 py-2 text-left font-semibold text-[var(--color-text)]">
                        Mode Development
                      </th>
                      <th className="border border-gray-300 px-4 py-2 text-left font-semibold text-[var(--color-text)]">
                        Mode Production
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Cluster
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Le cluster reste actif pour réutilisation rapide
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Le cluster est arrêté immédiatement après exécution
                      </td>
                    </tr>
                    <tr>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Reprise sur erreur
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Pas de retry automatique
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Retry automatique en cas d&apos;échec
                      </td>
                    </tr>
                    <tr>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Utilisation
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Développement et tests itératifs
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Exécution planifiée en production
                      </td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </div>

            {/* Event log info */}
            <div>
              <InfoBox type="info" title="Métriques des expectations dans l'event log">
                <p>
                  Toutes les métriques liées aux expectations (nombre
                  d&apos;enregistrements validés, rejetés, etc.) sont
                  automatiquement enregistrées dans l&apos;
                  <strong>event log</strong> du pipeline DLT. Vous pouvez
                  interroger ces métriques pour surveiller la qualité de vos
                  données au fil du temps. Nous verrons comment exploiter
                  l&apos;event log dans la leçon suivante.
                </p>
              </InfoBox>
            </div>
          </section>

          {/* Quiz */}
          <Quiz
            lessonSlug="4-1-delta-live-tables"
            title="Quiz — Delta Live Tables (DLT)"
            questions={quizQuestions}
          />

          {/* Exercices */}
          <LessonExercises
            lessonSlug="4-1-delta-live-tables"
            exercises={exercises}
          />

          {/* Complétion */}
          <LessonCompleteButton lessonSlug="4-1-delta-live-tables" />

          {/* Navigation */}
          <div className="flex flex-col sm:flex-row justify-between gap-4 mt-12 pt-8 border-t border-[var(--color-border)]">
            <Link
              href="/modules/3-3-architecture-multi-hop"
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
              Leçon précédente : Architecture Multi-Hop
            </Link>
            <Link
              href="/modules/4-2-resultats-pipeline"
              className="inline-flex items-center gap-2 px-5 py-2.5 bg-[#ff3621] text-white rounded-lg font-medium hover:bg-[#e02e1a] transition-colors"
            >
              Leçon suivante : Résultats du Pipeline
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
