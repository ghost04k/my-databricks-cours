"use client";

import { useState } from "react";
import Link from "next/link";
import CodeBlock from "@/components/CodeBlock";
import InfoBox from "@/components/InfoBox";

function SolutionToggle({
  id,
  children,
}: {
  id: string;
  children: React.ReactNode;
}) {
  const [open, setOpen] = useState(false);

  return (
    <div className="mt-4">
      <button
        onClick={() => setOpen(!open)}
        className="inline-flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-semibold bg-[#1b3a4b] text-white hover:bg-[#2d5f7a] transition-colors"
        aria-expanded={open}
        aria-controls={id}
      >
        {open ? "🙈 Masquer la solution" : "👁️ Voir la solution"}
      </button>
      {open && (
        <div
          id={id}
          className="mt-4 border-l-4 border-[#ff3621] pl-5 space-y-4"
        >
          {children}
        </div>
      )}
    </div>
  );
}

export default function ProductionGouvernanceExercicesPage() {
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
            <span className="inline-flex items-center px-3 py-1 rounded-full text-xs font-semibold bg-yellow-400/20 text-yellow-200 border border-yellow-400/30">
              Intermédiaire
            </span>
            <span className="text-sm text-white/70">⏱ 4 heures</span>
            <span className="text-sm text-white/70">
              📘 Modules 4 &amp; 5
            </span>
          </div>
          <h1 className="text-3xl lg:text-4xl font-extrabold mb-3">
            🏭 Exercices : Production &amp; Gouvernance
          </h1>
          <p className="text-lg text-white/80 max-w-2xl leading-relaxed">
            5 exercices progressifs pour maîtriser les pipelines Delta Live
            Tables (SQL &amp; Python), le monitoring, l&apos;orchestration avec
            Jobs et la gouvernance des données avec Unity Catalog.
          </p>
        </div>
      </div>

      {/* Content */}
      <div className="max-w-4xl mx-auto px-6 py-10 lg:px-10">
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

        {/* Sommaire */}
        <div className="bg-gray-50 rounded-xl border border-gray-200 p-5 mb-10">
          <h2 className="text-lg font-bold text-[#1b3a4b] mb-3">
            📋 Sommaire des exercices
          </h2>
          <ol className="space-y-2 text-sm text-gray-700">
            <li className="flex items-center gap-2">
              <span className="w-6 h-6 flex items-center justify-center bg-[#1b3a4b] text-white text-xs font-bold rounded-full">
                1
              </span>
              <span>
                Pipeline DLT en SQL{" "}
                <span className="text-gray-400">(1h)</span>
              </span>
            </li>
            <li className="flex items-center gap-2">
              <span className="w-6 h-6 flex items-center justify-center bg-[#1b3a4b] text-white text-xs font-bold rounded-full">
                2
              </span>
              <span>
                Pipeline DLT en Python{" "}
                <span className="text-gray-400">(1h)</span>
              </span>
            </li>
            <li className="flex items-center gap-2">
              <span className="w-6 h-6 flex items-center justify-center bg-[#1b3a4b] text-white text-xs font-bold rounded-full">
                3
              </span>
              <span>
                Monitoring DLT et Event Log{" "}
                <span className="text-gray-400">(30 min)</span>
              </span>
            </li>
            <li className="flex items-center gap-2">
              <span className="w-6 h-6 flex items-center justify-center bg-[#1b3a4b] text-white text-xs font-bold rounded-full">
                4
              </span>
              <span>
                Orchestration avec Jobs{" "}
                <span className="text-gray-400">(45 min)</span>
              </span>
            </li>
            <li className="flex items-center gap-2">
              <span className="w-6 h-6 flex items-center justify-center bg-[#1b3a4b] text-white text-xs font-bold rounded-full">
                5
              </span>
              <span>
                Unity Catalog &amp; Permissions{" "}
                <span className="text-gray-400">(45 min)</span>
              </span>
            </li>
          </ol>
        </div>

        {/* ====================== EXERCICE 1 ====================== */}
        <section className="mb-14">
          <div className="flex items-center gap-3 mb-2">
            <span className="w-8 h-8 flex items-center justify-center bg-[#ff3621] text-white text-sm font-bold rounded-full">
              1
            </span>
            <h2 className="text-2xl font-bold text-[#1b3a4b]">
              Pipeline DLT en SQL
            </h2>
          </div>
          <div className="flex items-center gap-3 mb-5 ml-11">
            <span className="text-xs font-medium bg-gray-100 text-gray-600 px-2.5 py-1 rounded-full">
              ⏱ 1 heure
            </span>
            <span className="text-xs font-medium bg-yellow-100 text-yellow-700 px-2.5 py-1 rounded-full">
              Intermédiaire
            </span>
          </div>

          <div className="ml-11 space-y-4">
            <h3 className="text-lg font-semibold text-[#1b3a4b]">
              📖 Contexte
            </h3>
            <p className="text-gray-700 leading-relaxed">
              Vous devez créer un pipeline Delta Live Tables complet pour
              traiter des données de transactions bancaires. Le pipeline suit
              l&apos;architecture Medallion : ingestion Bronze, nettoyage
              Silver avec contrôles de qualité, et agrégation Gold.
            </p>

            <h3 className="text-lg font-semibold text-[#1b3a4b]">
              📝 Instructions
            </h3>
            <ol className="list-decimal list-inside space-y-2 text-gray-700">
              <li>
                Créez une <strong>Streaming Live Table</strong> pour
                l&apos;ingestion Bronze des fichiers JSON de transactions.
              </li>
              <li>
                Créez une <strong>Live Table Silver</strong> avec nettoyage et
                ajout d&apos;un timestamp de traitement.
              </li>
              <li>
                Ajoutez des <strong>expectations de qualité</strong> pour
                valider les données (ID non nul, montant positif, type valide).
              </li>
              <li>
                Créez une <strong>table Gold</strong> d&apos;agrégation qui
                calcule le solde par compte.
              </li>
            </ol>

            <InfoBox type="info" title="À propos de Delta Live Tables">
              <p>
                DLT est un framework déclaratif de Databricks. Vous déclarez
                le résultat souhaité (la table), et DLT gère automatiquement
                l&apos;orchestration, les dépendances et la gestion des
                erreurs.
              </p>
            </InfoBox>

            <h3 className="text-lg font-semibold text-[#1b3a4b]">
              ✅ Résultat attendu
            </h3>
            <ul className="list-disc list-inside text-sm text-gray-700 space-y-1">
              <li>3 tables créées dans le pipeline (Bronze, Silver, Gold)</li>
              <li>Les lignes invalides sont rejetées au niveau Silver</li>
              <li>La table Gold contient les soldes agrégés par compte</li>
            </ul>

            <SolutionToggle id="sol-1">
              <p className="text-sm text-gray-700 mb-2">
                <strong>Solution complète en SQL :</strong>
              </p>

              <CodeBlock
                language="sql"
                title="Bronze : Ingestion des transactions"
                code={`-- Bronze : Ingestion des données brutes
CREATE OR REFRESH STREAMING TABLE transactions_bronze
AS SELECT * FROM cloud_files("/data/transactions", "json",
  map("cloudFiles.schemaHints", "transaction_id STRING, account_id STRING, amount DOUBLE, type STRING, timestamp TIMESTAMP"))`}
              />

              <p className="text-sm text-gray-700 mt-4">
                <strong>Explications :</strong>
              </p>
              <ul className="list-disc list-inside text-sm text-gray-600 space-y-1">
                <li>
                  <code>CREATE OR REFRESH STREAMING TABLE</code> : crée une
                  table en streaming incrémental via Auto Loader.
                </li>
                <li>
                  <code>cloud_files</code> : fonction Auto Loader qui détecte
                  automatiquement les nouveaux fichiers.
                </li>
                <li>
                  <code>schemaHints</code> : indique le schéma attendu pour
                  les fichiers JSON.
                </li>
              </ul>

              <CodeBlock
                language="sql"
                title="Silver : Nettoyage avec expectations"
                code={`-- Silver : Nettoyage avec contrôles de qualité
CREATE OR REFRESH STREAMING TABLE transactions_silver (
  CONSTRAINT valid_id EXPECT (transaction_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_amount EXPECT (amount > 0) ON VIOLATION DROP ROW,
  CONSTRAINT valid_type EXPECT (type IN ('credit', 'debit', 'transfer')) ON VIOLATION DROP ROW
)
AS SELECT
  transaction_id,
  account_id,
  amount,
  type,
  timestamp,
  current_timestamp() AS processed_at
FROM STREAM(LIVE.transactions_bronze)`}
              />

              <p className="text-sm text-gray-700 mt-4">
                <strong>Explications :</strong>
              </p>
              <ul className="list-disc list-inside text-sm text-gray-600 space-y-1">
                <li>
                  <code>CONSTRAINT ... EXPECT ... ON VIOLATION DROP ROW</code>{" "}
                  : supprime les lignes qui ne respectent pas la contrainte.
                </li>
                <li>
                  3 validations : ID non nul, montant strictement positif,
                  type parmi les valeurs autorisées.
                </li>
                <li>
                  <code>STREAM(LIVE.transactions_bronze)</code> : lit la table
                  Bronze en mode streaming.
                </li>
              </ul>

              <CodeBlock
                language="sql"
                title="Gold : Agrégation des soldes"
                code={`-- Gold : Soldes par compte
CREATE OR REFRESH LIVE TABLE account_balances
AS SELECT
  account_id,
  SUM(CASE WHEN type = 'credit' THEN amount ELSE -amount END) AS balance,
  COUNT(*) AS total_transactions,
  MAX(timestamp) AS last_transaction
FROM LIVE.transactions_silver
GROUP BY account_id`}
              />

              <p className="text-sm text-gray-700 mt-4">
                <strong>Explications :</strong>
              </p>
              <ul className="list-disc list-inside text-sm text-gray-600 space-y-1">
                <li>
                  <code>LIVE TABLE</code> (sans STREAMING) : table
                  matérialisée recalculée à chaque exécution.
                </li>
                <li>
                  Le CASE calcule le solde : les crédits s&apos;ajoutent, les
                  débits et transferts se soustraient.
                </li>
                <li>
                  <code>LIVE.transactions_silver</code> : référence la table
                  Silver du même pipeline.
                </li>
              </ul>

              <InfoBox type="tip" title="Astuce">
                <p>
                  Utilisez <code>ON VIOLATION FAIL UPDATE</code> au lieu de{" "}
                  <code>DROP ROW</code> si vous voulez que le pipeline
                  échoue en cas de données invalides plutôt que de les
                  supprimer silencieusement.
                </p>
              </InfoBox>
            </SolutionToggle>
          </div>
        </section>

        {/* ====================== EXERCICE 2 ====================== */}
        <section className="mb-14">
          <div className="flex items-center gap-3 mb-2">
            <span className="w-8 h-8 flex items-center justify-center bg-[#ff3621] text-white text-sm font-bold rounded-full">
              2
            </span>
            <h2 className="text-2xl font-bold text-[#1b3a4b]">
              Pipeline DLT en Python
            </h2>
          </div>
          <div className="flex items-center gap-3 mb-5 ml-11">
            <span className="text-xs font-medium bg-gray-100 text-gray-600 px-2.5 py-1 rounded-full">
              ⏱ 1 heure
            </span>
            <span className="text-xs font-medium bg-yellow-100 text-yellow-700 px-2.5 py-1 rounded-full">
              Intermédiaire
            </span>
          </div>

          <div className="ml-11 space-y-4">
            <h3 className="text-lg font-semibold text-[#1b3a4b]">
              📖 Contexte
            </h3>
            <p className="text-gray-700 leading-relaxed">
              Reproduisez le même pipeline de transactions bancaires que
              l&apos;exercice 1, mais cette fois en utilisant la syntaxe
              Python avec les décorateurs DLT. Cela vous permet de comparer
              les deux approches et de choisir la plus adaptée selon vos
              besoins.
            </p>

            <h3 className="text-lg font-semibold text-[#1b3a4b]">
              📝 Instructions
            </h3>
            <ol className="list-decimal list-inside space-y-2 text-gray-700">
              <li>
                Importez le module <code>dlt</code> et les fonctions PySpark
                nécessaires.
              </li>
              <li>
                Créez une fonction <code>transactions_bronze()</code> avec le
                décorateur <code>@dlt.table</code> pour l&apos;ingestion.
              </li>
              <li>
                Créez une fonction <code>transactions_silver()</code> avec les
                décorateurs <code>@dlt.expect_or_drop</code> pour la
                validation.
              </li>
              <li>
                Créez une fonction <code>account_balances()</code> pour
                l&apos;agrégation Gold.
              </li>
            </ol>

            <InfoBox type="info" title="SQL vs Python pour DLT">
              <p>
                Les deux syntaxes produisent le même résultat. SQL est plus
                concis pour les transformations simples, tandis que Python
                offre plus de flexibilité pour la logique complexe (boucles,
                conditions, appels API, etc.).
              </p>
            </InfoBox>

            <h3 className="text-lg font-semibold text-[#1b3a4b]">
              ✅ Résultat attendu
            </h3>
            <ul className="list-disc list-inside text-sm text-gray-700 space-y-1">
              <li>Le pipeline produit les mêmes 3 tables qu&apos;en SQL</li>
              <li>Les expectations filtrent les données invalides</li>
              <li>Les soldes par compte sont correctement calculés</li>
            </ul>

            <SolutionToggle id="sol-2">
              <p className="text-sm text-gray-700 mb-2">
                <strong>Solution complète en Python :</strong>
              </p>

              <CodeBlock
                language="python"
                title="Pipeline DLT complet en Python"
                code={`import dlt
from pyspark.sql.functions import col, sum, count, max, when, current_timestamp

@dlt.table(
    comment="Transactions brutes ingérées depuis les fichiers JSON"
)
def transactions_bronze():
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load("/data/transactions"))

@dlt.table(
    comment="Transactions nettoyées et validées"
)
@dlt.expect_or_drop("valid_id", "transaction_id IS NOT NULL")
@dlt.expect_or_drop("valid_amount", "amount > 0")
@dlt.expect("valid_type", "type IN ('credit', 'debit', 'transfer')")
def transactions_silver():
    return (dlt.readStream("transactions_bronze")
        .withColumn("processed_at", current_timestamp()))

@dlt.table(
    comment="Soldes agrégés par compte"
)
def account_balances():
    return (dlt.read("transactions_silver")
        .groupBy("account_id")
        .agg(
            sum(when(col("type") == "credit", col("amount")).otherwise(-col("amount"))).alias("balance"),
            count("*").alias("total_transactions"),
            max("timestamp").alias("last_transaction")
        ))`}
              />

              <p className="text-sm text-gray-700 mt-4">
                <strong>Explications :</strong>
              </p>
              <ul className="list-disc list-inside text-sm text-gray-600 space-y-1">
                <li>
                  <code>@dlt.table</code> : décorateur qui déclare une
                  fonction comme une table DLT. Le nom de la fonction
                  devient le nom de la table.
                </li>
                <li>
                  <code>@dlt.expect_or_drop</code> : supprime les lignes qui
                  ne passent pas la validation (équivalent de{" "}
                  <code>ON VIOLATION DROP ROW</code>).
                </li>
                <li>
                  <code>@dlt.expect</code> (sans <code>_or_drop</code>) :
                  logue les violations mais conserve les lignes.
                </li>
                <li>
                  <code>dlt.readStream()</code> : lit une table DLT en mode
                  streaming (pour les tables Streaming).
                </li>
                <li>
                  <code>dlt.read()</code> : lit une table DLT en mode batch
                  (pour les tables matérialisées).
                </li>
              </ul>

              <InfoBox type="warning" title="Attention">
                <p>
                  Ne confondez pas <code>dlt.expect</code> et{" "}
                  <code>dlt.expect_or_drop</code>. Le premier enregistre la
                  violation mais garde la ligne, le second la supprime. Pour
                  des données critiques, utilisez{" "}
                  <code>dlt.expect_or_fail</code> pour arrêter le pipeline.
                </p>
              </InfoBox>
            </SolutionToggle>
          </div>
        </section>

        {/* ====================== EXERCICE 3 ====================== */}
        <section className="mb-14">
          <div className="flex items-center gap-3 mb-2">
            <span className="w-8 h-8 flex items-center justify-center bg-[#ff3621] text-white text-sm font-bold rounded-full">
              3
            </span>
            <h2 className="text-2xl font-bold text-[#1b3a4b]">
              Monitoring DLT et Event Log
            </h2>
          </div>
          <div className="flex items-center gap-3 mb-5 ml-11">
            <span className="text-xs font-medium bg-gray-100 text-gray-600 px-2.5 py-1 rounded-full">
              ⏱ 30 min
            </span>
            <span className="text-xs font-medium bg-yellow-100 text-yellow-700 px-2.5 py-1 rounded-full">
              Intermédiaire
            </span>
          </div>

          <div className="ml-11 space-y-4">
            <h3 className="text-lg font-semibold text-[#1b3a4b]">
              📖 Contexte
            </h3>
            <p className="text-gray-700 leading-relaxed">
              Après avoir exécuté votre pipeline DLT, vous devez analyser
              ses résultats. L&apos;Event Log de DLT contient toutes les
              métriques d&apos;exécution, les statistiques de qualité des
              données et les erreurs éventuelles.
            </p>

            <h3 className="text-lg font-semibold text-[#1b3a4b]">
              📝 Instructions
            </h3>
            <ol className="list-decimal list-inside space-y-2 text-gray-700">
              <li>
                Interrogez l&apos;Event Log pour consulter les métriques
                de qualité des expectations.
              </li>
              <li>
                Vérifiez le nombre de lignes rejetées par les contraintes
                de validation.
              </li>
              <li>
                Analysez les performances d&apos;exécution du pipeline.
              </li>
            </ol>

            <InfoBox type="tip" title="Event Log">
              <p>
                L&apos;Event Log est une table Delta en lecture seule qui
                capture chaque événement du pipeline. Utilisez la fonction{" "}
                <code>event_log()</code> pour y accéder avec le nom de
                votre pipeline.
              </p>
            </InfoBox>

            <h3 className="text-lg font-semibold text-[#1b3a4b]">
              ✅ Résultat attendu
            </h3>
            <ul className="list-disc list-inside text-sm text-gray-700 space-y-1">
              <li>
                Visualisation des métriques de qualité (lignes acceptées /
                rejetées)
              </li>
              <li>
                Nombre de lignes traitées à chaque étape du pipeline
              </li>
              <li>
                Détails sur les violations de contraintes
              </li>
            </ul>

            <SolutionToggle id="sol-3">
              <p className="text-sm text-gray-700 mb-2">
                <strong>
                  Requêtes d&apos;analyse de l&apos;Event Log :
                </strong>
              </p>

              <CodeBlock
                language="sql"
                title="Métriques de qualité des expectations"
                code={`-- Voir les métriques de qualité des expectations
SELECT
  details:flow_progress.metrics.num_output_rows AS rows_output,
  details:flow_progress.data_quality.expectations AS quality_metrics
FROM event_log("transactions_pipeline")
WHERE event_type = 'flow_progress'
ORDER BY timestamp DESC
LIMIT 10;`}
              />

              <p className="text-sm text-gray-700 mt-4">
                <strong>Explications :</strong>
              </p>
              <ul className="list-disc list-inside text-sm text-gray-600 space-y-1">
                <li>
                  <code>event_log(&quot;transactions_pipeline&quot;)</code> :
                  accède à l&apos;Event Log du pipeline nommé.
                </li>
                <li>
                  <code>details:flow_progress</code> : utilise la notation
                  JSON pour accéder aux champs imbriqués.
                </li>
                <li>
                  <code>event_type = &apos;flow_progress&apos;</code> : filtre
                  sur les événements de progression du flux.
                </li>
              </ul>

              <CodeBlock
                language="sql"
                title="Vérification des records rejetés"
                code={`-- Vérifier les records rejetés par les expectations
SELECT
  details:flow_progress.data_quality.dropped_records AS dropped,
  details:flow_progress.data_quality.expected_records AS expected
FROM event_log("transactions_pipeline")
WHERE event_type = 'flow_progress'
  AND details:flow_progress.data_quality IS NOT NULL;`}
              />

              <p className="text-sm text-gray-700 mt-4">
                <strong>Explications :</strong>
              </p>
              <ul className="list-disc list-inside text-sm text-gray-600 space-y-1">
                <li>
                  <code>dropped_records</code> : nombre de lignes supprimées
                  par les expectations <code>DROP ROW</code>.
                </li>
                <li>
                  <code>expected_records</code> : nombre de lignes
                  évaluées par les contraintes.
                </li>
                <li>
                  Le filtre <code>data_quality IS NOT NULL</code> exclut les
                  événements sans métriques de qualité.
                </li>
              </ul>

              <InfoBox type="important" title="Important">
                <p>
                  L&apos;Event Log est disponible uniquement après la
                  première exécution du pipeline. Le nom passé à{" "}
                  <code>event_log()</code> doit correspondre exactement au
                  nom du pipeline configuré dans l&apos;interface DLT.
                </p>
              </InfoBox>
            </SolutionToggle>
          </div>
        </section>

        {/* ====================== EXERCICE 4 ====================== */}
        <section className="mb-14">
          <div className="flex items-center gap-3 mb-2">
            <span className="w-8 h-8 flex items-center justify-center bg-[#ff3621] text-white text-sm font-bold rounded-full">
              4
            </span>
            <h2 className="text-2xl font-bold text-[#1b3a4b]">
              Orchestration avec Jobs
            </h2>
          </div>
          <div className="flex items-center gap-3 mb-5 ml-11">
            <span className="text-xs font-medium bg-gray-100 text-gray-600 px-2.5 py-1 rounded-full">
              ⏱ 45 min
            </span>
            <span className="text-xs font-medium bg-yellow-100 text-yellow-700 px-2.5 py-1 rounded-full">
              Intermédiaire
            </span>
          </div>

          <div className="ml-11 space-y-4">
            <h3 className="text-lg font-semibold text-[#1b3a4b]">
              📖 Contexte
            </h3>
            <p className="text-gray-700 leading-relaxed">
              Vous devez orchestrer un workflow complet de traitement de
              données en créant un Job multi-tâches avec des dépendances
              entre les étapes : ingestion → pipeline DLT → validation →
              notification.
            </p>

            <h3 className="text-lg font-semibold text-[#1b3a4b]">
              📝 Instructions
            </h3>
            <ol className="list-decimal list-inside space-y-2 text-gray-700">
              <li>
                <strong>Task 1 - Ingestion :</strong> Créez un notebook qui
                ingère les nouvelles données avec <code>COPY INTO</code>.
              </li>
              <li>
                <strong>Task 2 - Pipeline DLT :</strong> Configurez une tâche
                DLT dans l&apos;interface Jobs (pas de code nécessaire).
              </li>
              <li>
                <strong>Task 3 - Validation :</strong> Vérifiez que le
                pipeline a bien produit des données et transmettez le résultat.
              </li>
              <li>
                <strong>Task 4 - Notification :</strong> Récupérez le résultat
                de la tâche précédente et affichez un résumé.
              </li>
            </ol>

            <InfoBox type="info" title="Architecture du Job">
              <p>
                Les Jobs Databricks permettent de créer des DAGs (graphes
                orientés acycliques) de tâches. Chaque tâche peut être un
                notebook, un pipeline DLT, un script Python, etc. Les
                dépendances définissent l&apos;ordre d&apos;exécution.
              </p>
            </InfoBox>

            <h3 className="text-lg font-semibold text-[#1b3a4b]">
              ✅ Résultat attendu
            </h3>
            <ul className="list-disc list-inside text-sm text-gray-700 space-y-1">
              <li>Un Job avec 4 tâches connectées par des dépendances</li>
              <li>L&apos;exécution séquentielle : ingestion → DLT → validation → notification</li>
              <li>Passage de valeurs entre les tâches via <code>taskValues</code></li>
            </ul>

            <SolutionToggle id="sol-4">
              <p className="text-sm text-gray-700 mb-2">
                <strong>Solution : Code des notebooks du Job</strong>
              </p>

              <CodeBlock
                language="python"
                title="Task 1 : Notebook d'ingestion"
                code={`# Task 1 : Ingestion des nouvelles données
# Ce notebook déclenche l'ingestion des fichiers dans la table raw
spark.sql("""
    COPY INTO transactions_raw
    FROM '/new_data/'
    FILEFORMAT = JSON
""")`}
              />

              <p className="text-sm text-gray-700 mt-4">
                <strong>Explications :</strong>
              </p>
              <ul className="list-disc list-inside text-sm text-gray-600 space-y-1">
                <li>
                  <code>COPY INTO</code> : commande idempotente qui n&apos;ingère
                  que les fichiers non encore traités.
                </li>
                <li>
                  La Task 2 (DLT) est configurée directement dans
                  l&apos;interface de Jobs, aucun code n&apos;est nécessaire.
                </li>
              </ul>

              <CodeBlock
                language="python"
                title="Task 3 : Notebook de validation"
                code={`# Task 3 : Validation (dépend de Task 2 - Pipeline DLT)
row_count = spark.sql("SELECT COUNT(*) FROM transactions_silver").collect()[0][0]

if row_count == 0:
    dbutils.notebook.exit("FAILED: No data processed")

# Passer le résultat à la tâche suivante via taskValues
dbutils.jobs.taskValues.set(key="row_count", value=row_count)
print(f"Validation OK : {row_count} lignes dans la table Silver.")`}
              />

              <p className="text-sm text-gray-700 mt-4">
                <strong>Explications :</strong>
              </p>
              <ul className="list-disc list-inside text-sm text-gray-600 space-y-1">
                <li>
                  <code>dbutils.notebook.exit()</code> : termine le notebook
                  avec un message. Utile pour signaler un échec.
                </li>
                <li>
                  <code>dbutils.jobs.taskValues.set()</code> : stocke une
                  valeur accessible par les tâches suivantes du même Job.
                </li>
              </ul>

              <CodeBlock
                language="python"
                title="Task 4 : Notebook de notification"
                code={`# Task 4 : Notification (dépend de Task 3 - Validation)
count = dbutils.jobs.taskValues.get(taskKey="validation", key="row_count")
print(f"Pipeline terminé avec succès. {count} lignes traitées.")`}
              />

              <p className="text-sm text-gray-700 mt-4">
                <strong>Explications :</strong>
              </p>
              <ul className="list-disc list-inside text-sm text-gray-600 space-y-1">
                <li>
                  <code>dbutils.jobs.taskValues.get()</code> : récupère la
                  valeur stockée par une tâche précédente.
                </li>
                <li>
                  <code>taskKey=&quot;validation&quot;</code> : le nom de la
                  tâche qui a stocké la valeur.
                </li>
              </ul>

              <InfoBox type="tip" title="Astuce">
                <p>
                  Dans l&apos;interface de Jobs, configurez des{" "}
                  <strong>alertes email</strong> sur les échecs pour être
                  notifié automatiquement. Vous pouvez aussi ajouter des
                  politiques de <strong>retry</strong> (ré-essai automatique)
                  sur chaque tâche.
                </p>
              </InfoBox>
            </SolutionToggle>
          </div>
        </section>

        {/* ====================== EXERCICE 5 ====================== */}
        <section className="mb-14">
          <div className="flex items-center gap-3 mb-2">
            <span className="w-8 h-8 flex items-center justify-center bg-[#ff3621] text-white text-sm font-bold rounded-full">
              5
            </span>
            <h2 className="text-2xl font-bold text-[#1b3a4b]">
              Unity Catalog &amp; Permissions
            </h2>
          </div>
          <div className="flex items-center gap-3 mb-5 ml-11">
            <span className="text-xs font-medium bg-gray-100 text-gray-600 px-2.5 py-1 rounded-full">
              ⏱ 45 min
            </span>
            <span className="text-xs font-medium bg-yellow-100 text-yellow-700 px-2.5 py-1 rounded-full">
              Intermédiaire
            </span>
          </div>

          <div className="ml-11 space-y-4">
            <h3 className="text-lg font-semibold text-[#1b3a4b]">
              📖 Contexte
            </h3>
            <p className="text-gray-700 leading-relaxed">
              Vous devez configurer la gouvernance des données pour une
              application bancaire. Cela inclut la création d&apos;un
              catalogue Unity Catalog, la gestion des permissions par rôle,
              et la mise en place de vues sécurisées avec masquage de
              données sensibles.
            </p>

            <h3 className="text-lg font-semibold text-[#1b3a4b]">
              📝 Instructions
            </h3>
            <ol className="list-decimal list-inside space-y-2 text-gray-700">
              <li>
                Créez un <strong>catalogue</strong> et un{" "}
                <strong>schéma</strong> dans Unity Catalog.
              </li>
              <li>
                Créez une table <code>customers</code> avec des données
                sensibles (email, numéro de sécurité sociale).
              </li>
              <li>
                Configurez les <strong>permissions</strong> pour le groupe
                &quot;analysts&quot; (accès en lecture seule).
              </li>
              <li>
                Créez une <strong>vue sécurisée</strong> avec masquage des
                colonnes sensibles selon le rôle de l&apos;utilisateur.
              </li>
              <li>
                Vérifiez les permissions attribuées avec{" "}
                <code>SHOW GRANTS</code>.
              </li>
            </ol>

            <InfoBox type="warning" title="Prérequis">
              <p>
                Unity Catalog doit être activé sur votre workspace
                Databricks. Vous devez avoir les droits{" "}
                <strong>CREATE CATALOG</strong> au niveau du metastore pour
                cet exercice.
              </p>
            </InfoBox>

            <h3 className="text-lg font-semibold text-[#1b3a4b]">
              ✅ Résultat attendu
            </h3>
            <ul className="list-disc list-inside text-sm text-gray-700 space-y-1">
              <li>
                Structure hiérarchique : Catalogue → Schéma → Tables
              </li>
              <li>
                Permissions fines : les analystes ne voient que les données
                autorisées
              </li>
              <li>
                Les données sensibles (email, SSN) sont masquées pour les
                utilisateurs non autorisés
              </li>
            </ul>

            <SolutionToggle id="sol-5">
              <p className="text-sm text-gray-700 mb-2">
                <strong>Solution complète :</strong>
              </p>

              <CodeBlock
                language="sql"
                title="Création de la structure Unity Catalog"
                code={`-- Créer la structure Unity Catalog
CREATE CATALOG IF NOT EXISTS finance_catalog;
USE CATALOG finance_catalog;

CREATE SCHEMA IF NOT EXISTS banking;
USE SCHEMA banking;

-- Créer la table clients avec données sensibles
CREATE TABLE IF NOT EXISTS customers (
  customer_id STRING,
  name STRING,
  email STRING,
  ssn STRING,
  risk_level STRING
);`}
              />

              <p className="text-sm text-gray-700 mt-4">
                <strong>Explications :</strong>
              </p>
              <ul className="list-disc list-inside text-sm text-gray-600 space-y-1">
                <li>
                  <code>CREATE CATALOG</code> : crée le niveau supérieur de
                  la hiérarchie Unity Catalog.
                </li>
                <li>
                  <code>CREATE SCHEMA</code> : crée un schéma (base de
                  données) à l&apos;intérieur du catalogue.
                </li>
                <li>
                  La hiérarchie est : Metastore → Catalogue → Schéma →
                  Table/Vue.
                </li>
              </ul>

              <CodeBlock
                language="sql"
                title="Configuration des permissions"
                code={`-- Accorder les permissions au groupe analysts
GRANT USAGE ON CATALOG finance_catalog TO analysts;
GRANT USAGE ON SCHEMA finance_catalog.banking TO analysts;
GRANT SELECT ON TABLE finance_catalog.banking.customers TO analysts;`}
              />

              <p className="text-sm text-gray-700 mt-4">
                <strong>Explications :</strong>
              </p>
              <ul className="list-disc list-inside text-sm text-gray-600 space-y-1">
                <li>
                  <code>GRANT USAGE</code> : autorise l&apos;accès au
                  catalogue ou schéma (sans voir les données).
                </li>
                <li>
                  <code>GRANT SELECT</code> : autorise la lecture des données
                  de la table.
                </li>
                <li>
                  Les permissions sont cumulatives : il faut USAGE sur le
                  catalogue ET le schéma, puis SELECT sur la table.
                </li>
              </ul>

              <CodeBlock
                language="sql"
                title="Vue sécurisée avec masquage dynamique"
                code={`-- Vue sécurisée avec masquage selon le rôle
CREATE OR REPLACE VIEW customers_secure AS
SELECT
  customer_id,
  name,
  CASE WHEN is_member('admins') THEN email
       ELSE CONCAT(LEFT(email, 2), '***@***') END AS email,
  CASE WHEN is_member('compliance') THEN ssn
       ELSE 'XXX-XX-XXXX' END AS ssn,
  risk_level
FROM customers;

-- Vérifier les permissions accordées
SHOW GRANTS ON TABLE finance_catalog.banking.customers;`}
              />

              <p className="text-sm text-gray-700 mt-4">
                <strong>Explications :</strong>
              </p>
              <ul className="list-disc list-inside text-sm text-gray-600 space-y-1">
                <li>
                  <code>is_member(&apos;admins&apos;)</code> : vérifie si
                  l&apos;utilisateur courant appartient au groupe spécifié.
                </li>
                <li>
                  Le masquage est <strong>dynamique</strong> : la même vue
                  affiche des données différentes selon le rôle de
                  l&apos;utilisateur connecté.
                </li>
                <li>
                  Les emails sont partiellement masqués (seules les 2
                  premières lettres sont visibles).
                </li>
                <li>
                  Les numéros de sécurité sociale sont entièrement masqués
                  pour les non-membres du groupe compliance.
                </li>
              </ul>

              <InfoBox type="important" title="Bonnes pratiques">
                <p>
                  En production, ne donnez jamais accès directement aux
                  tables contenant des données sensibles. Utilisez toujours
                  des <strong>vues sécurisées</strong> avec masquage
                  dynamique. Pensez aussi à activer l&apos;
                  <strong>audit log</strong> pour tracer les accès aux
                  données.
                </p>
              </InfoBox>
            </SolutionToggle>
          </div>
        </section>

        {/* Navigation Bas de page */}
        <div className="mt-16 pt-8 border-t border-gray-200">
          <div className="flex flex-col sm:flex-row justify-between gap-4">
            <Link
              href="/exercices"
              className="inline-flex items-center gap-2 px-5 py-3 rounded-xl bg-gray-100 text-[#1b3a4b] hover:bg-gray-200 transition-colors font-medium text-sm"
            >
              ← Tous les exercices
            </Link>
            <Link
              href="/programme"
              className="inline-flex items-center gap-2 px-5 py-3 rounded-xl bg-[#1b3a4b] text-white hover:bg-[#2d5f7a] transition-colors font-medium text-sm"
            >
              📅 Voir le programme complet →
            </Link>
          </div>
        </div>
      </div>
    </div>
  );
}
