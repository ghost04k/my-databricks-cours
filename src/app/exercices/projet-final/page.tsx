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

export default function ProjetFinalSNCFPage() {
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
            <span className="inline-flex items-center px-3 py-1 rounded-full text-xs font-semibold bg-red-400/20 text-red-200 border border-red-400/30">
              Avancé
            </span>
            <span className="text-sm text-white/70">⏱ 14 heures</span>
            <span className="text-sm text-white/70">📅 Jours 11–13</span>
          </div>
          <h1 className="text-3xl lg:text-4xl font-extrabold mb-3">
            🚄 Projet Final : Cas SNCF
          </h1>
          <p className="text-lg text-white/80 max-w-2xl leading-relaxed">
            Concevez et implémentez une plateforme data complète pour la SNCF
            — de l&apos;ingestion des données de trafic ferroviaire à
            l&apos;orchestration en production, en passant par la gouvernance
            Unity Catalog.
          </p>
        </div>
      </div>

      {/* Content */}
      <div className="max-w-4xl mx-auto px-6 py-10 lg:px-10">
        {/* Navigation / Breadcrumb */}
        <div className="flex flex-wrap gap-3 mb-10">
          <Link
            href="/exercices"
            className="inline-flex items-center gap-2 text-sm text-[#1b3a4b] hover:text-[#ff3621] transition-colors"
          >
            ← Exercices
          </Link>
          <span className="text-gray-300">|</span>
          <span className="text-sm text-gray-500">Projet Final</span>
        </div>

        {/* ──────────── Contexte du projet ──────────── */}
        <section className="mb-12">
          <h2 className="text-2xl font-bold text-[#1b3a4b] mb-4">
            📖 Contexte du projet
          </h2>
          <p className="text-gray-700 leading-relaxed mb-4">
            La <strong>SNCF</strong> souhaite moderniser sa plateforme data en
            migrant vers <strong>Databricks</strong>. Vous êtes recruté comme{" "}
            <strong>Data Engineer</strong> pour concevoir et implémenter un
            pipeline de données complet pour analyser les données de trafic
            ferroviaire, les retards, la maintenance des trains et la
            satisfaction client.
          </p>

          <div className="bg-gray-50 rounded-xl border border-gray-200 p-5 mb-6">
            <h3 className="text-lg font-bold text-[#1b3a4b] mb-3">
              🎯 Objectifs du projet
            </h3>
            <ul className="space-y-2 text-sm text-gray-700">
              <li className="flex items-center gap-2">
                <span className="text-green-500">✓</span>
                Concevoir une architecture Lakehouse Medallion complète
              </li>
              <li className="flex items-center gap-2">
                <span className="text-green-500">✓</span>
                Ingérer 6 sources de données multi-format (JSON, CSV, Parquet)
              </li>
              <li className="flex items-center gap-2">
                <span className="text-green-500">✓</span>
                Transformer et enrichir les données avec Delta Live Tables
              </li>
              <li className="flex items-center gap-2">
                <span className="text-green-500">✓</span>
                Créer des agrégations Gold métier (ponctualité, maintenance, satisfaction)
              </li>
              <li className="flex items-center gap-2">
                <span className="text-green-500">✓</span>
                Configurer Unity Catalog avec permissions granulaires
              </li>
              <li className="flex items-center gap-2">
                <span className="text-green-500">✓</span>
                Orchestrer le pipeline avec monitoring et alertes
              </li>
            </ul>
          </div>

          <div className="bg-white rounded-xl border border-gray-200 p-5 mb-6">
            <h3 className="text-lg font-bold text-[#1b3a4b] mb-3">
              📊 Sources de données SNCF
            </h3>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-gray-200">
                    <th className="text-left py-2 pr-4 font-semibold text-[#1b3a4b]">Source</th>
                    <th className="text-left py-2 pr-4 font-semibold text-[#1b3a4b]">Format</th>
                    <th className="text-left py-2 pr-4 font-semibold text-[#1b3a4b]">Volume</th>
                    <th className="text-left py-2 font-semibold text-[#1b3a4b]">Fréquence</th>
                  </tr>
                </thead>
                <tbody className="text-gray-700">
                  <tr className="border-b border-gray-100">
                    <td className="py-2 pr-4">🚆 Trains</td>
                    <td className="py-2 pr-4"><code className="bg-blue-50 text-blue-700 px-1.5 py-0.5 rounded text-xs">JSON</code></td>
                    <td className="py-2 pr-4">~15 000 trains</td>
                    <td className="py-2">Quotidien</td>
                  </tr>
                  <tr className="border-b border-gray-100">
                    <td className="py-2 pr-4">🏛️ Gares</td>
                    <td className="py-2 pr-4"><code className="bg-blue-50 text-blue-700 px-1.5 py-0.5 rounded text-xs">JSON</code></td>
                    <td className="py-2 pr-4">~3 000 gares</td>
                    <td className="py-2">Référentiel</td>
                  </tr>
                  <tr className="border-b border-gray-100">
                    <td className="py-2 pr-4">🛤️ Trajets</td>
                    <td className="py-2 pr-4"><code className="bg-green-50 text-green-700 px-1.5 py-0.5 rounded text-xs">CSV</code></td>
                    <td className="py-2 pr-4">~2M / mois</td>
                    <td className="py-2">Streaming</td>
                  </tr>
                  <tr className="border-b border-gray-100">
                    <td className="py-2 pr-4">⏱️ Retards</td>
                    <td className="py-2 pr-4"><code className="bg-green-50 text-green-700 px-1.5 py-0.5 rounded text-xs">CSV</code></td>
                    <td className="py-2 pr-4">~500K / mois</td>
                    <td className="py-2">Streaming</td>
                  </tr>
                  <tr className="border-b border-gray-100">
                    <td className="py-2 pr-4">🔧 Maintenance</td>
                    <td className="py-2 pr-4"><code className="bg-purple-50 text-purple-700 px-1.5 py-0.5 rounded text-xs">Parquet</code></td>
                    <td className="py-2 pr-4">~100K / mois</td>
                    <td className="py-2">Batch</td>
                  </tr>
                  <tr>
                    <td className="py-2 pr-4">⭐ Satisfaction</td>
                    <td className="py-2 pr-4"><code className="bg-purple-50 text-purple-700 px-1.5 py-0.5 rounded text-xs">Parquet</code></td>
                    <td className="py-2 pr-4">~300K / mois</td>
                    <td className="py-2">Batch</td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>
        </section>

        {/* ──────────── Architecture Medallion ──────────── */}
        <section className="mb-12">
          <h2 className="text-2xl font-bold text-[#1b3a4b] mb-4">
            🏗️ Architecture du projet
          </h2>
          <div className="bg-white rounded-xl border border-gray-200 p-6 overflow-x-auto">
            <div className="flex items-center justify-between gap-3 min-w-[700px]">
              {/* Sources */}
              <div className="flex flex-col items-center gap-2">
                <div className="bg-purple-100 border-2 border-purple-300 rounded-xl px-4 py-3 text-center min-w-[120px]">
                  <div className="text-2xl mb-1">📁</div>
                  <div className="font-bold text-purple-800 text-sm">Sources</div>
                  <div className="text-xs text-purple-600 mt-1">JSON / CSV / Parquet</div>
                </div>
              </div>

              <div className="text-2xl text-gray-400 font-bold">→</div>

              {/* Bronze */}
              <div className="flex flex-col items-center gap-2">
                <div className="bg-amber-100 border-2 border-amber-300 rounded-xl px-4 py-3 text-center min-w-[120px]">
                  <div className="text-2xl mb-1">🥉</div>
                  <div className="font-bold text-amber-800 text-sm">Bronze</div>
                  <div className="text-xs text-amber-600 mt-1">6 tables brutes</div>
                </div>
              </div>

              <div className="text-2xl text-gray-400 font-bold">→</div>

              {/* Silver */}
              <div className="flex flex-col items-center gap-2">
                <div className="bg-slate-100 border-2 border-slate-300 rounded-xl px-4 py-3 text-center min-w-[120px]">
                  <div className="text-2xl mb-1">🥈</div>
                  <div className="font-bold text-slate-700 text-sm">Silver</div>
                  <div className="text-xs text-slate-500 mt-1">Nettoyé + Enrichi</div>
                </div>
              </div>

              <div className="text-2xl text-gray-400 font-bold">→</div>

              {/* Gold */}
              <div className="flex flex-col items-center gap-2">
                <div className="bg-yellow-100 border-2 border-yellow-300 rounded-xl px-4 py-3 text-center min-w-[120px]">
                  <div className="text-2xl mb-1">🥇</div>
                  <div className="font-bold text-yellow-800 text-sm">Gold</div>
                  <div className="text-xs text-yellow-600 mt-1">KPIs métier</div>
                </div>
              </div>

              <div className="text-2xl text-gray-400 font-bold">→</div>

              {/* BI */}
              <div className="flex flex-col items-center gap-2">
                <div className="bg-green-100 border-2 border-green-300 rounded-xl px-4 py-3 text-center min-w-[120px]">
                  <div className="text-2xl mb-1">📊</div>
                  <div className="font-bold text-green-800 text-sm">BI / ML</div>
                  <div className="text-xs text-green-600 mt-1">Dashboards</div>
                </div>
              </div>
            </div>
          </div>
        </section>

        {/* ══════════════════════════════════════════════════════════════
            PHASE 1 — Architecture et Setup
        ══════════════════════════════════════════════════════════════ */}
        <section className="mb-12">
          <div className="flex items-center gap-3 mb-6">
            <span className="inline-flex items-center justify-center w-10 h-10 rounded-full bg-[#1b3a4b] text-white font-bold text-lg">1</span>
            <div>
              <h2 className="text-2xl font-bold text-[#1b3a4b]">
                Phase 1 — Architecture et Setup
              </h2>
              <p className="text-sm text-gray-500">Jour 11 matin · 2 heures</p>
            </div>
          </div>

          <div className="bg-white rounded-xl border border-gray-200 p-6 mb-6">
            <h3 className="text-lg font-bold text-[#1b3a4b] mb-3">
              🏗️ Exercice 1 : Concevoir l&apos;architecture Lakehouse SNCF
            </h3>
            <p className="text-gray-700 mb-4">
              Créez la structure complète du catalogue Unity Catalog pour la SNCF.
              Définissez le schéma des données sources et les trois couches Medallion.
            </p>

            <div className="bg-gray-50 rounded-lg p-4 mb-4">
              <h4 className="font-semibold text-[#1b3a4b] mb-2">📋 Tâches :</h4>
              <ul className="space-y-1.5 text-sm text-gray-700">
                <li>1. Créer le catalogue <code className="bg-gray-200 px-1.5 py-0.5 rounded text-xs">sncf_catalog</code> avec les schémas Bronze, Silver, Gold</li>
                <li>2. Définir le schéma de chaque source (trains, gares, trajets, retards, maintenance, satisfaction)</li>
                <li>3. Documenter les flux de données entre les couches</li>
              </ul>
            </div>

            <SolutionToggle id="sol-1">
              <p className="font-semibold text-[#1b3a4b] mb-2">
                Création du catalogue et des schémas :
              </p>
              <CodeBlock language="sql" code={`-- Création du catalogue SNCF
CREATE CATALOG IF NOT EXISTS sncf_catalog;
USE CATALOG sncf_catalog;

-- Création des schémas Medallion
CREATE SCHEMA IF NOT EXISTS bronze
  COMMENT 'Données brutes ingérées sans transformation';
CREATE SCHEMA IF NOT EXISTS silver
  COMMENT 'Données nettoyées, dédupliquées et enrichies';
CREATE SCHEMA IF NOT EXISTS gold
  COMMENT 'Agrégations métier pour le BI et le reporting';`} />

              <p className="font-semibold text-[#1b3a4b] mt-6 mb-2">
                Schéma des tables sources :
              </p>
              <CodeBlock language="sql" code={`-- Table de référence des trains
CREATE TABLE IF NOT EXISTS bronze.trains (
  train_id STRING,
  type_train STRING,        -- TGV, TER, Intercités, Ouigo
  capacite INT,
  date_mise_service DATE,
  constructeur STRING,      -- Alstom, Siemens
  statut STRING,            -- en_service, maintenance, hors_service
  _ingestion_timestamp TIMESTAMP,
  _source_file STRING
);

-- Table de référence des gares
CREATE TABLE IF NOT EXISTS bronze.gares (
  gare_id STRING,
  nom_gare STRING,          -- Paris Gare de Lyon, Marseille Saint-Charles
  ville STRING,
  region STRING,
  latitude DOUBLE,
  longitude DOUBLE,
  nb_quais INT,
  est_gare_tgv BOOLEAN,
  _ingestion_timestamp TIMESTAMP,
  _source_file STRING
);

-- Table des trajets
CREATE TABLE IF NOT EXISTS bronze.trajets (
  trajet_id STRING,
  train_id STRING,
  ligne STRING,             -- Paris-Lyon, Paris-Marseille
  gare_depart STRING,
  gare_arrivee STRING,
  date_trajet DATE,
  heure_depart_prevue TIMESTAMP,
  heure_arrivee_prevue TIMESTAMP,
  heure_depart_reelle TIMESTAMP,
  heure_arrivee_reelle TIMESTAMP,
  nb_passagers INT,
  _ingestion_timestamp TIMESTAMP,
  _source_file STRING
);

-- Table des retards
CREATE TABLE IF NOT EXISTS bronze.retards (
  retard_id STRING,
  trajet_id STRING,
  cause_retard STRING,      -- technique, météo, grève, voyageur, infrastructure
  retard_depart_minutes INT,
  retard_arrivee_minutes INT,
  commentaire STRING,
  _ingestion_timestamp TIMESTAMP,
  _source_file STRING
);

-- Table de maintenance
CREATE TABLE IF NOT EXISTS bronze.maintenance (
  maintenance_id STRING,
  train_id STRING,
  type_maintenance STRING,  -- préventive, corrective, révision_générale
  date_debut DATE,
  date_fin DATE,
  cout_euros DOUBLE,
  piece_remplacee STRING,
  technicien STRING,
  _ingestion_timestamp TIMESTAMP,
  _source_file STRING
);

-- Table de satisfaction client
CREATE TABLE IF NOT EXISTS bronze.satisfaction (
  enquete_id STRING,
  trajet_id STRING,
  email_client STRING,
  score_global INT,         -- 1 à 10
  score_confort INT,
  score_ponctualite INT,
  score_info_voyageur INT,
  score_proprete INT,
  commentaire_libre STRING,
  date_enquete DATE,
  _ingestion_timestamp TIMESTAMP,
  _source_file STRING
);`} />
            </SolutionToggle>
          </div>
        </section>

        {/* ══════════════════════════════════════════════════════════════
            PHASE 2 — Ingestion Bronze
        ══════════════════════════════════════════════════════════════ */}
        <section className="mb-12">
          <div className="flex items-center gap-3 mb-6">
            <span className="inline-flex items-center justify-center w-10 h-10 rounded-full bg-[#1b3a4b] text-white font-bold text-lg">2</span>
            <div>
              <h2 className="text-2xl font-bold text-[#1b3a4b]">
                Phase 2 — Ingestion Bronze
              </h2>
              <p className="text-sm text-gray-500">Jour 11 après-midi · 3 heures</p>
            </div>
          </div>

          <div className="bg-white rounded-xl border border-gray-200 p-6 mb-6">
            <h3 className="text-lg font-bold text-[#1b3a4b] mb-3">
              📥 Exercice 2 : Ingestion des données brutes avec Auto Loader
            </h3>
            <p className="text-gray-700 mb-4">
              Ingérez les 6 sources de données dans la couche Bronze en utilisant
              Auto Loader et Delta Live Tables. Chaque source a un format différent
              et doit être traitée avec les bonnes options.
            </p>

            <div className="bg-gray-50 rounded-lg p-4 mb-4">
              <h4 className="font-semibold text-[#1b3a4b] mb-2">📋 Tâches :</h4>
              <ul className="space-y-1.5 text-sm text-gray-700">
                <li>1. Ingérer les fichiers JSON (trains, gares) avec Auto Loader</li>
                <li>2. Ingérer les fichiers CSV (trajets, retards) en streaming</li>
                <li>3. Ingérer les fichiers Parquet (maintenance, satisfaction)</li>
                <li>4. Ajouter les métadonnées d&apos;ingestion (<code className="bg-gray-200 px-1.5 py-0.5 rounded text-xs">_ingestion_timestamp</code>, <code className="bg-gray-200 px-1.5 py-0.5 rounded text-xs">_source_file</code>)</li>
              </ul>
            </div>

            <SolutionToggle id="sol-2">
              <p className="font-semibold text-[#1b3a4b] mb-2">
                Pipeline DLT — Ingestion Bronze complète :
              </p>
              <CodeBlock language="python" code={`import dlt
from pyspark.sql import functions as F

# ── Trains (JSON) ─────────────────────────
@dlt.table(
    comment="Données brutes des trains SNCF",
    table_properties={"quality": "bronze"}
)
def bronze_trains():
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "/mnt/sncf/schemas/trains")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("/mnt/sncf/raw/trains/")
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )

# ── Gares (JSON) ──────────────────────────
@dlt.table(
    comment="Référentiel des gares SNCF",
    table_properties={"quality": "bronze"}
)
def bronze_gares():
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "/mnt/sncf/schemas/gares")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("/mnt/sncf/raw/gares/")
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )`} />

              <CodeBlock language="python" code={`# ── Trajets (CSV) ─────────────────────────
@dlt.table(
    comment="Données brutes des trajets SNCF",
    table_properties={"quality": "bronze"}
)
def bronze_trajets():
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", "/mnt/sncf/schemas/trajets")
        .option("header", "true")
        .option("sep", ";")
        .load("/mnt/sncf/raw/trajets/")
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )

# ── Retards (CSV) ─────────────────────────
@dlt.table(
    comment="Données brutes des retards",
    table_properties={"quality": "bronze"}
)
def bronze_retards():
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", "/mnt/sncf/schemas/retards")
        .option("header", "true")
        .option("sep", ";")
        .load("/mnt/sncf/raw/retards/")
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )`} />

              <CodeBlock language="python" code={`# ── Maintenance (Parquet) ──────────────────
@dlt.table(
    comment="Données brutes de maintenance des trains",
    table_properties={"quality": "bronze"}
)
def bronze_maintenance():
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", "/mnt/sncf/schemas/maintenance")
        .load("/mnt/sncf/raw/maintenance/")
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )

# ── Satisfaction (Parquet) ─────────────────
@dlt.table(
    comment="Enquêtes de satisfaction client",
    table_properties={"quality": "bronze"}
)
def bronze_satisfaction():
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", "/mnt/sncf/schemas/satisfaction")
        .load("/mnt/sncf/raw/satisfaction/")
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )`} />
            </SolutionToggle>
          </div>
        </section>

        {/* ══════════════════════════════════════════════════════════════
            PHASE 3 — Transformation Silver
        ══════════════════════════════════════════════════════════════ */}
        <section className="mb-12">
          <div className="flex items-center gap-3 mb-6">
            <span className="inline-flex items-center justify-center w-10 h-10 rounded-full bg-[#1b3a4b] text-white font-bold text-lg">3</span>
            <div>
              <h2 className="text-2xl font-bold text-[#1b3a4b]">
                Phase 3 — Transformation Silver
              </h2>
              <p className="text-sm text-gray-500">Jour 12 matin · 3 heures</p>
            </div>
          </div>

          <div className="bg-white rounded-xl border border-gray-200 p-6 mb-6">
            <h3 className="text-lg font-bold text-[#1b3a4b] mb-3">
              🔧 Exercice 3 : Nettoyage et enrichissement des données
            </h3>
            <p className="text-gray-700 mb-4">
              Nettoyez, dédupliquez et enrichissez les données Bronze pour
              créer des tables Silver fiables. Utilisez les expectations DLT
              pour garantir la qualité des données.
            </p>

            <div className="bg-gray-50 rounded-lg p-4 mb-4">
              <h4 className="font-semibold text-[#1b3a4b] mb-2">📋 Tâches :</h4>
              <ul className="space-y-1.5 text-sm text-gray-700">
                <li>1. Dédupliquer les trajets par <code className="bg-gray-200 px-1.5 py-0.5 rounded text-xs">trajet_id</code></li>
                <li>2. Joindre les trajets avec les retards</li>
                <li>3. Calculer la durée réelle vs prévue et le flag <code className="bg-gray-200 px-1.5 py-0.5 rounded text-xs">est_en_retard</code></li>
                <li>4. Enrichir les trajets avec les infos gare (ville, région)</li>
                <li>5. Nettoyer et valider les données de maintenance</li>
                <li>6. Normaliser les scores de satisfaction (1–10)</li>
              </ul>
            </div>

            <SolutionToggle id="sol-3">
              <p className="font-semibold text-[#1b3a4b] mb-2">
                Table Silver — Trajets enrichis avec retards :
              </p>
              <CodeBlock language="python" code={`@dlt.table(
    comment="Trajets enrichis avec informations de retard",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("trajet_valide", "trajet_id IS NOT NULL")
@dlt.expect_or_drop("gare_depart_valide", "gare_depart IS NOT NULL")
@dlt.expect("duree_positive", "duree_reelle_minutes > 0")
def silver_trajets_enrichis():
    trajets = dlt.read_stream("bronze_trajets")
    retards = dlt.read("bronze_retards")
    gares = dlt.read("bronze_gares")
    
    return (trajets
        .dropDuplicates(["trajet_id"])
        # Jointure avec les retards
        .join(retards, "trajet_id", "left")
        # Calcul durée réelle et retard
        .withColumn("duree_prevue_minutes",
            (F.col("heure_arrivee_prevue").cast("long") - 
             F.col("heure_depart_prevue").cast("long")) / 60)
        .withColumn("duree_reelle_minutes",
            (F.col("heure_arrivee_reelle").cast("long") - 
             F.col("heure_depart_reelle").cast("long")) / 60)
        .withColumn("retard_minutes",
            F.coalesce(F.col("retard_arrivee_minutes"), F.lit(0)))
        .withColumn("est_en_retard", F.col("retard_minutes") > 5)
        # Enrichissement avec info gare départ
        .join(
            gares.select(
                F.col("gare_id"),
                F.col("ville").alias("ville_depart"),
                F.col("region").alias("region_depart")
            ),
            F.col("gare_depart") == F.col("gare_id"),
            "left"
        )
        .drop("gare_id")
    )`} />

              <p className="font-semibold text-[#1b3a4b] mt-6 mb-2">
                Table Silver — Maintenance nettoyée :
              </p>
              <CodeBlock language="python" code={`@dlt.table(
    comment="Données de maintenance nettoyées et validées",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("maintenance_valide", "maintenance_id IS NOT NULL")
@dlt.expect_or_drop("train_valide", "train_id IS NOT NULL")
@dlt.expect("cout_positif", "cout_euros >= 0")
@dlt.expect("dates_coherentes", "date_fin >= date_debut")
def silver_maintenance():
    maintenance = dlt.read_stream("bronze_maintenance")
    trains = dlt.read("bronze_trains")
    
    return (maintenance
        .dropDuplicates(["maintenance_id"])
        .join(trains.select("train_id", "type_train", "constructeur"),
              "train_id", "left")
        .withColumn("duree_maintenance_jours",
            F.datediff(F.col("date_fin"), F.col("date_debut")))
        .withColumn("est_maintenance_longue",
            F.col("duree_maintenance_jours") > 7)
    )`} />

              <p className="font-semibold text-[#1b3a4b] mt-6 mb-2">
                Table Silver — Satisfaction normalisée :
              </p>
              <CodeBlock language="python" code={`@dlt.table(
    comment="Satisfaction client normalisée",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("enquete_valide", "enquete_id IS NOT NULL")
@dlt.expect("score_dans_plage", "score_global BETWEEN 1 AND 10")
def silver_satisfaction():
    satisfaction = dlt.read_stream("bronze_satisfaction")
    
    return (satisfaction
        .dropDuplicates(["enquete_id"])
        .withColumn("score_global", 
            F.when(F.col("score_global") > 10, 10)
             .when(F.col("score_global") < 1, 1)
             .otherwise(F.col("score_global")))
        .withColumn("categorie_satisfaction",
            F.when(F.col("score_global") >= 8, "Très satisfait")
             .when(F.col("score_global") >= 6, "Satisfait")
             .when(F.col("score_global") >= 4, "Neutre")
             .otherwise("Insatisfait"))
    )`} />
            </SolutionToggle>
          </div>
        </section>

        {/* ══════════════════════════════════════════════════════════════
            PHASE 4 — Agrégation Gold
        ══════════════════════════════════════════════════════════════ */}
        <section className="mb-12">
          <div className="flex items-center gap-3 mb-6">
            <span className="inline-flex items-center justify-center w-10 h-10 rounded-full bg-[#1b3a4b] text-white font-bold text-lg">4</span>
            <div>
              <h2 className="text-2xl font-bold text-[#1b3a4b]">
                Phase 4 — Agrégation Gold
              </h2>
              <p className="text-sm text-gray-500">Jour 12 après-midi · 2 heures</p>
            </div>
          </div>

          <div className="bg-white rounded-xl border border-gray-200 p-6 mb-6">
            <h3 className="text-lg font-bold text-[#1b3a4b] mb-3">
              🥇 Exercice 4 : Tables d&apos;agrégation pour le métier
            </h3>
            <p className="text-gray-700 mb-4">
              Créez les tables Gold qui alimenteront les dashboards métier
              de la SNCF. Chaque table doit répondre à un besoin business précis.
            </p>

            <div className="bg-gray-50 rounded-lg p-4 mb-4">
              <h4 className="font-semibold text-[#1b3a4b] mb-2">📋 Tables Gold à créer :</h4>
              <ul className="space-y-1.5 text-sm text-gray-700">
                <li>1. <strong>gold_ponctualite_par_ligne</strong> — Taux de ponctualité par ligne, gare et mois</li>
                <li>2. <strong>gold_maintenance_predictive</strong> — Trains avec incidents fréquents (candidats à la maintenance préventive)</li>
                <li>3. <strong>gold_satisfaction_par_axe</strong> — Score satisfaction par axe (confort, ponctualité, info voyageur)</li>
              </ul>
            </div>

            <SolutionToggle id="sol-4">
              <p className="font-semibold text-[#1b3a4b] mb-2">
                Table Gold — Ponctualité par ligne :
              </p>
              <CodeBlock language="sql" code={`CREATE OR REFRESH LIVE TABLE gold_ponctualite_par_ligne
COMMENT 'Taux de ponctualité par ligne et par mois - Dashboard Direction'
AS
SELECT
  ligne,
  region_depart,
  MONTH(date_trajet) AS mois,
  YEAR(date_trajet) AS annee,
  COUNT(*) AS total_trajets,
  SUM(CASE WHEN est_en_retard = false THEN 1 ELSE 0 END) AS trajets_ponctuels,
  ROUND(
    SUM(CASE WHEN est_en_retard = false THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2
  ) AS taux_ponctualite,
  ROUND(AVG(retard_minutes), 1) AS retard_moyen_minutes,
  MAX(retard_minutes) AS retard_max_minutes,
  ROUND(AVG(nb_passagers), 0) AS passagers_moyen
FROM LIVE.silver_trajets_enrichis
GROUP BY ligne, region_depart, MONTH(date_trajet), YEAR(date_trajet);`} />

              <p className="font-semibold text-[#1b3a4b] mt-6 mb-2">
                Table Gold — Maintenance prédictive :
              </p>
              <CodeBlock language="sql" code={`CREATE OR REFRESH LIVE TABLE gold_maintenance_predictive
COMMENT 'Trains à risque nécessitant une maintenance préventive'
AS
SELECT
  m.train_id,
  t.type_train,
  t.constructeur,
  t.date_mise_service,
  COUNT(*) AS nb_interventions_12_mois,
  SUM(m.cout_euros) AS cout_total_maintenance,
  ROUND(AVG(m.duree_maintenance_jours), 1) AS duree_moyenne_intervention,
  SUM(CASE WHEN m.type_maintenance = 'corrective' THEN 1 ELSE 0 END) AS nb_correctifs,
  MAX(m.date_fin) AS derniere_maintenance,
  DATEDIFF(CURRENT_DATE(), MAX(m.date_fin)) AS jours_depuis_derniere_maintenance,
  CASE
    WHEN SUM(CASE WHEN m.type_maintenance = 'corrective' THEN 1 ELSE 0 END) >= 3 
      THEN 'CRITIQUE'
    WHEN SUM(CASE WHEN m.type_maintenance = 'corrective' THEN 1 ELSE 0 END) >= 2 
      THEN 'ATTENTION'
    ELSE 'NORMAL'
  END AS niveau_risque
FROM LIVE.silver_maintenance m
JOIN LIVE.bronze_trains t ON m.train_id = t.train_id
WHERE m.date_debut >= ADD_MONTHS(CURRENT_DATE(), -12)
GROUP BY m.train_id, t.type_train, t.constructeur, t.date_mise_service
HAVING COUNT(*) >= 2;`} />

              <p className="font-semibold text-[#1b3a4b] mt-6 mb-2">
                Table Gold — Satisfaction par axe :
              </p>
              <CodeBlock language="sql" code={`CREATE OR REFRESH LIVE TABLE gold_satisfaction_par_axe
COMMENT 'Scores de satisfaction moyens par axe et par ligne'
AS
SELECT
  te.ligne,
  te.region_depart AS region,
  MONTH(s.date_enquete) AS mois,
  YEAR(s.date_enquete) AS annee,
  COUNT(*) AS nb_reponses,
  ROUND(AVG(s.score_global), 2) AS score_global_moyen,
  ROUND(AVG(s.score_confort), 2) AS score_confort_moyen,
  ROUND(AVG(s.score_ponctualite), 2) AS score_ponctualite_moyen,
  ROUND(AVG(s.score_info_voyageur), 2) AS score_info_moyen,
  ROUND(AVG(s.score_proprete), 2) AS score_proprete_moyen,
  ROUND(
    SUM(CASE WHEN s.categorie_satisfaction = 'Très satisfait' THEN 1 ELSE 0 END) 
    * 100.0 / COUNT(*), 1
  ) AS pct_tres_satisfait,
  ROUND(
    SUM(CASE WHEN s.categorie_satisfaction = 'Insatisfait' THEN 1 ELSE 0 END) 
    * 100.0 / COUNT(*), 1
  ) AS pct_insatisfait
FROM LIVE.silver_satisfaction s
JOIN LIVE.silver_trajets_enrichis te ON s.trajet_id = te.trajet_id
GROUP BY te.ligne, te.region_depart, MONTH(s.date_enquete), YEAR(s.date_enquete);`} />
            </SolutionToggle>
          </div>
        </section>

        {/* ══════════════════════════════════════════════════════════════
            PHASE 5 — Gouvernance et Sécurité
        ══════════════════════════════════════════════════════════════ */}
        <section className="mb-12">
          <div className="flex items-center gap-3 mb-6">
            <span className="inline-flex items-center justify-center w-10 h-10 rounded-full bg-[#1b3a4b] text-white font-bold text-lg">5</span>
            <div>
              <h2 className="text-2xl font-bold text-[#1b3a4b]">
                Phase 5 — Gouvernance et Sécurité
              </h2>
              <p className="text-sm text-gray-500">Jour 13 matin · 2 heures</p>
            </div>
          </div>

          <div className="bg-white rounded-xl border border-gray-200 p-6 mb-6">
            <h3 className="text-lg font-bold text-[#1b3a4b] mb-3">
              🔐 Exercice 5 : Configurer Unity Catalog et les permissions
            </h3>
            <p className="text-gray-700 mb-4">
              Mettez en place la gouvernance des données avec Unity Catalog.
              Définissez les rôles, configurez les accès par couche et masquez
              les données sensibles pour les profils non autorisés.
            </p>

            <div className="bg-gray-50 rounded-lg p-4 mb-4">
              <h4 className="font-semibold text-[#1b3a4b] mb-2">📋 Tâches :</h4>
              <ul className="space-y-1.5 text-sm text-gray-700">
                <li>1. Créer les groupes : <code className="bg-gray-200 px-1.5 py-0.5 rounded text-xs">data_engineers</code>, <code className="bg-gray-200 px-1.5 py-0.5 rounded text-xs">data_analysts</code>, <code className="bg-gray-200 px-1.5 py-0.5 rounded text-xs">data_scientists</code>, <code className="bg-gray-200 px-1.5 py-0.5 rounded text-xs">managers</code></li>
                <li>2. Configurer les accès par couche (analysts = Gold uniquement, engineers = toutes couches)</li>
                <li>3. Créer une vue sécurisée pour anonymiser les données de satisfaction</li>
                <li>4. Configurer le row-level security pour les managers régionaux</li>
              </ul>
            </div>

            <SolutionToggle id="sol-5">
              <p className="font-semibold text-[#1b3a4b] mb-2">
                Permissions par rôle :
              </p>
              <CodeBlock language="sql" code={`-- ═══════════════════════════════════════════
-- Rôle Data Engineer : accès complet
-- ═══════════════════════════════════════════
GRANT USAGE ON CATALOG sncf_catalog TO data_engineers;
GRANT USAGE ON SCHEMA sncf_catalog.bronze TO data_engineers;
GRANT USAGE ON SCHEMA sncf_catalog.silver TO data_engineers;
GRANT USAGE ON SCHEMA sncf_catalog.gold TO data_engineers;
GRANT SELECT, MODIFY ON SCHEMA sncf_catalog.bronze TO data_engineers;
GRANT SELECT, MODIFY ON SCHEMA sncf_catalog.silver TO data_engineers;
GRANT SELECT, MODIFY ON SCHEMA sncf_catalog.gold TO data_engineers;

-- ═══════════════════════════════════════════
-- Rôle Data Analyst : accès Gold uniquement
-- ═══════════════════════════════════════════
GRANT USAGE ON CATALOG sncf_catalog TO data_analysts;
GRANT USAGE ON SCHEMA sncf_catalog.gold TO data_analysts;
GRANT SELECT ON SCHEMA sncf_catalog.gold TO data_analysts;

-- ═══════════════════════════════════════════
-- Rôle Data Scientist : accès Silver + Gold
-- ═══════════════════════════════════════════
GRANT USAGE ON CATALOG sncf_catalog TO data_scientists;
GRANT USAGE ON SCHEMA sncf_catalog.silver TO data_scientists;
GRANT USAGE ON SCHEMA sncf_catalog.gold TO data_scientists;
GRANT SELECT ON SCHEMA sncf_catalog.silver TO data_scientists;
GRANT SELECT ON SCHEMA sncf_catalog.gold TO data_scientists;

-- ═══════════════════════════════════════════
-- Rôle Manager : accès Gold (lecture seule)
-- ═══════════════════════════════════════════
GRANT USAGE ON CATALOG sncf_catalog TO managers;
GRANT USAGE ON SCHEMA sncf_catalog.gold TO managers;
GRANT SELECT ON SCHEMA sncf_catalog.gold TO managers;`} />

              <p className="font-semibold text-[#1b3a4b] mt-6 mb-2">
                Vue sécurisée — Satisfaction anonymisée :
              </p>
              <CodeBlock language="sql" code={`-- Vue dynamique qui masque les données sensibles
-- selon le groupe de l'utilisateur connecté
CREATE OR REPLACE VIEW sncf_catalog.gold.satisfaction_anonymisee AS
SELECT
  ligne,
  region,
  mois,
  annee,
  nb_reponses,
  score_global_moyen,
  score_confort_moyen,
  score_ponctualite_moyen,
  score_info_moyen,
  score_proprete_moyen,
  -- Email masqué pour les non-ingénieurs
  CASE 
    WHEN is_member('data_engineers') THEN email_client 
    ELSE 'ANONYME' 
  END AS email_client,
  -- Commentaires visibles uniquement pour engineers + scientists
  CASE 
    WHEN is_member('data_engineers') OR is_member('data_scientists') 
      THEN commentaire_libre
    ELSE NULL 
  END AS commentaire_libre
FROM sncf_catalog.silver.silver_satisfaction s
JOIN sncf_catalog.silver.silver_trajets_enrichis t 
  ON s.trajet_id = t.trajet_id;`} />

              <p className="font-semibold text-[#1b3a4b] mt-6 mb-2">
                Row-level security — Managers par région :
              </p>
              <CodeBlock language="sql" code={`-- Les managers régionaux ne voient que les données de leur région
CREATE OR REPLACE FUNCTION sncf_catalog.gold.filtre_region()
RETURNS STRING
RETURN CASE
  WHEN is_member('managers_idf') THEN 'Île-de-France'
  WHEN is_member('managers_paca') THEN 'Provence-Alpes-Côte d\\'Azur'
  WHEN is_member('managers_aura') THEN 'Auvergne-Rhône-Alpes'
  WHEN is_member('data_engineers') THEN 'ALL'
  ELSE 'NONE'
END;

-- Vue filtrée par région du manager
CREATE OR REPLACE VIEW sncf_catalog.gold.ponctualite_par_region AS
SELECT *
FROM sncf_catalog.gold.gold_ponctualite_par_ligne
WHERE sncf_catalog.gold.filtre_region() = 'ALL'
   OR region_depart = sncf_catalog.gold.filtre_region();`} />
            </SolutionToggle>
          </div>
        </section>

        {/* ══════════════════════════════════════════════════════════════
            PHASE 6 — Orchestration et Monitoring
        ══════════════════════════════════════════════════════════════ */}
        <section className="mb-12">
          <div className="flex items-center gap-3 mb-6">
            <span className="inline-flex items-center justify-center w-10 h-10 rounded-full bg-[#1b3a4b] text-white font-bold text-lg">6</span>
            <div>
              <h2 className="text-2xl font-bold text-[#1b3a4b]">
                Phase 6 — Orchestration et Monitoring
              </h2>
              <p className="text-sm text-gray-500">Jour 13 après-midi · 2 heures</p>
            </div>
          </div>

          <div className="bg-white rounded-xl border border-gray-200 p-6 mb-6">
            <h3 className="text-lg font-bold text-[#1b3a4b] mb-3">
              ⚙️ Exercice 6 : Job multi-tâches avec monitoring
            </h3>
            <p className="text-gray-700 mb-4">
              Créez un workflow Databricks complet qui orchestre tout le pipeline :
              ingestion, transformation, agrégation, contrôle qualité.
              Configurez les alertes et les task values pour le monitoring.
            </p>

            <div className="bg-gray-50 rounded-lg p-4 mb-4">
              <h4 className="font-semibold text-[#1b3a4b] mb-2">📋 Tâches :</h4>
              <ul className="space-y-1.5 text-sm text-gray-700">
                <li>1. Créer un workflow multi-tâches : ingestion → transformation → agrégation → quality check</li>
                <li>2. Configurer les dépendances entre tâches</li>
                <li>3. Transmettre des valeurs entre tâches via <code className="bg-gray-200 px-1.5 py-0.5 rounded text-xs">dbutils.jobs.taskValues</code></li>
                <li>4. Configurer les alertes email en cas d&apos;échec ou de dégradation qualité</li>
              </ul>
            </div>

            <SolutionToggle id="sol-6">
              <p className="font-semibold text-[#1b3a4b] mb-2">
                Architecture du workflow :
              </p>
              <div className="bg-white rounded-lg border border-gray-200 p-4 mb-4 overflow-x-auto">
                <div className="flex items-center gap-3 min-w-[650px] text-sm">
                  <div className="bg-amber-100 border border-amber-300 rounded-lg px-3 py-2 text-center">
                    <div className="font-bold text-amber-800">Task 1</div>
                    <div className="text-xs text-amber-600">DLT Pipeline</div>
                    <div className="text-xs text-amber-600">Ingestion</div>
                  </div>
                  <div className="text-gray-400 font-bold">→</div>
                  <div className="bg-slate-100 border border-slate-300 rounded-lg px-3 py-2 text-center">
                    <div className="font-bold text-slate-700">Task 2</div>
                    <div className="text-xs text-slate-500">DLT Pipeline</div>
                    <div className="text-xs text-slate-500">Transformation</div>
                  </div>
                  <div className="text-gray-400 font-bold">→</div>
                  <div className="bg-yellow-100 border border-yellow-300 rounded-lg px-3 py-2 text-center">
                    <div className="font-bold text-yellow-800">Task 3</div>
                    <div className="text-xs text-yellow-600">DLT Pipeline</div>
                    <div className="text-xs text-yellow-600">Agrégation</div>
                  </div>
                  <div className="text-gray-400 font-bold">→</div>
                  <div className="bg-green-100 border border-green-300 rounded-lg px-3 py-2 text-center">
                    <div className="font-bold text-green-800">Task 4</div>
                    <div className="text-xs text-green-600">Notebook</div>
                    <div className="text-xs text-green-600">Quality Check</div>
                  </div>
                  <div className="text-gray-400 font-bold">→</div>
                  <div className="bg-blue-100 border border-blue-300 rounded-lg px-3 py-2 text-center">
                    <div className="font-bold text-blue-800">Task 5</div>
                    <div className="text-xs text-blue-600">Notebook</div>
                    <div className="text-xs text-blue-600">Notification</div>
                  </div>
                </div>
              </div>

              <p className="font-semibold text-[#1b3a4b] mt-4 mb-2">
                Task 4 — Quality Check avec Task Values :
              </p>
              <CodeBlock language="python" code={`# ── Quality Check : Vérification de la couche Gold ──
from pyspark.sql import functions as F

# Vérification de la table de ponctualité
ponctualite_stats = spark.sql("""
    SELECT 
        COUNT(*) AS total_rows,
        SUM(CASE WHEN taux_ponctualite IS NULL THEN 1 ELSE 0 END) AS null_ponctualite,
        SUM(CASE WHEN total_trajets = 0 THEN 1 ELSE 0 END) AS lignes_sans_trajets,
        MIN(taux_ponctualite) AS min_ponctualite,
        MAX(taux_ponctualite) AS max_ponctualite,
        ROUND(AVG(taux_ponctualite), 2) AS avg_ponctualite
    FROM sncf_catalog.gold.gold_ponctualite_par_ligne
""").first()

# Vérification de la table de maintenance
maintenance_stats = spark.sql("""
    SELECT 
        COUNT(*) AS total_trains,
        SUM(CASE WHEN niveau_risque = 'CRITIQUE' THEN 1 ELSE 0 END) AS trains_critiques,
        SUM(CASE WHEN niveau_risque = 'ATTENTION' THEN 1 ELSE 0 END) AS trains_attention
    FROM sncf_catalog.gold.gold_maintenance_predictive
""").first()

# Calcul du score qualité global
quality_score = round(
    (1 - ponctualite_stats.null_ponctualite / ponctualite_stats.total_rows) * 100, 2
)

# Transmission des valeurs aux tâches suivantes
dbutils.jobs.taskValues.set(key="quality_score", value=quality_score)
dbutils.jobs.taskValues.set(key="total_rows_ponctualite", value=ponctualite_stats.total_rows)
dbutils.jobs.taskValues.set(key="avg_ponctualite", value=float(ponctualite_stats.avg_ponctualite))
dbutils.jobs.taskValues.set(key="trains_critiques", value=maintenance_stats.trains_critiques)

print(f"✅ Quality Score : {quality_score}%")
print(f"📊 Lignes ponctualité : {ponctualite_stats.total_rows}")
print(f"🚆 Ponctualité moyenne : {ponctualite_stats.avg_ponctualite}%")
print(f"⚠️  Trains critiques : {maintenance_stats.trains_critiques}")

# Échec si trop de valeurs nulles
if ponctualite_stats.null_ponctualite / ponctualite_stats.total_rows > 0.05:
    raise Exception(
        f"❌ Quality check FAILED : {ponctualite_stats.null_ponctualite} "
        f"valeurs nulles sur {ponctualite_stats.total_rows} lignes "
        f"({round(ponctualite_stats.null_ponctualite/ponctualite_stats.total_rows*100,1)}%)"
    )`} />

              <p className="font-semibold text-[#1b3a4b] mt-6 mb-2">
                Task 5 — Notification et rapport :
              </p>
              <CodeBlock language="python" code={`# ── Récupération des valeurs de la tâche précédente ──
quality_score = dbutils.jobs.taskValues.get(
    taskKey="quality_check", key="quality_score"
)
total_rows = dbutils.jobs.taskValues.get(
    taskKey="quality_check", key="total_rows_ponctualite"
)
avg_ponctualite = dbutils.jobs.taskValues.get(
    taskKey="quality_check", key="avg_ponctualite"
)
trains_critiques = dbutils.jobs.taskValues.get(
    taskKey="quality_check", key="trains_critiques"
)

# Génération du rapport
rapport = f"""
══════════════════════════════════════════
  RAPPORT PIPELINE SNCF — {datetime.now().strftime('%d/%m/%Y %H:%M')}
══════════════════════════════════════════

📊 Score qualité global : {quality_score}%
📈 Lignes de ponctualité : {total_rows}
🚄 Ponctualité moyenne : {avg_ponctualite}%
⚠️  Trains en maintenance critique : {trains_critiques}

Statut : {'✅ SUCCÈS' if quality_score >= 95 else '⚠️ DÉGRADÉ'}
══════════════════════════════════════════
"""

print(rapport)

# Alerte si dégradation
if quality_score < 95:
    # Envoi d'une notification (webhook, email, Slack...)
    import requests
    requests.post(
        "https://hooks.slack.com/services/SNCF_WEBHOOK",
        json={"text": f"⚠️ Pipeline SNCF dégradé — Score : {quality_score}%"}
    )`} />

              <p className="font-semibold text-[#1b3a4b] mt-6 mb-2">
                Configuration JSON du workflow :
              </p>
              <CodeBlock language="json" code={`{
  "name": "SNCF_Pipeline_Quotidien",
  "schedule": {
    "quartz_cron_expression": "0 0 6 * * ?",
    "timezone_id": "Europe/Paris"
  },
  "email_notifications": {
    "on_failure": ["data-team@sncf.fr"],
    "on_success": ["monitoring@sncf.fr"]
  },
  "tasks": [
    {
      "task_key": "ingestion_bronze",
      "pipeline_task": { "pipeline_id": "sncf-bronze-pipeline" }
    },
    {
      "task_key": "transformation_silver",
      "depends_on": [{ "task_key": "ingestion_bronze" }],
      "pipeline_task": { "pipeline_id": "sncf-silver-pipeline" }
    },
    {
      "task_key": "agregation_gold",
      "depends_on": [{ "task_key": "transformation_silver" }],
      "pipeline_task": { "pipeline_id": "sncf-gold-pipeline" }
    },
    {
      "task_key": "quality_check",
      "depends_on": [{ "task_key": "agregation_gold" }],
      "notebook_task": {
        "notebook_path": "/SNCF/jobs/quality_check"
      }
    },
    {
      "task_key": "notification",
      "depends_on": [{ "task_key": "quality_check" }],
      "notebook_task": {
        "notebook_path": "/SNCF/jobs/notification"
      }
    }
  ],
  "max_concurrent_runs": 1,
  "timeout_seconds": 7200
}`} />
            </SolutionToggle>
          </div>
        </section>

        {/* ══════════════════════════════════════════════════════════════
            Critères de réussite
        ══════════════════════════════════════════════════════════════ */}
        <section className="mb-12">
          <h2 className="text-2xl font-bold text-[#1b3a4b] mb-4">
            🏆 Critères de réussite
          </h2>
          <div className="bg-gradient-to-br from-green-50 to-emerald-50 rounded-xl border border-green-200 p-6">
            <p className="text-gray-700 mb-4">
              Votre projet est considéré comme <strong>réussi</strong> si vous
              avez implémenté les éléments suivants :
            </p>
            <ul className="space-y-2.5 text-gray-700">
              <li className="flex items-center gap-3">
                <span className="text-green-500 text-lg">✅</span>
                Architecture Medallion avec 3 couches (Bronze, Silver, Gold)
              </li>
              <li className="flex items-center gap-3">
                <span className="text-green-500 text-lg">✅</span>
                Auto Loader pour l&apos;ingestion streaming multi-format
              </li>
              <li className="flex items-center gap-3">
                <span className="text-green-500 text-lg">✅</span>
                Delta Live Tables avec expectations de qualité
              </li>
              <li className="flex items-center gap-3">
                <span className="text-green-500 text-lg">✅</span>
                Jointures et transformations complexes (trajets + retards + gares)
              </li>
              <li className="flex items-center gap-3">
                <span className="text-green-500 text-lg">✅</span>
                Agrégations Gold prêtes pour le BI (ponctualité, maintenance, satisfaction)
              </li>
              <li className="flex items-center gap-3">
                <span className="text-green-500 text-lg">✅</span>
                Unity Catalog avec permissions granulaires par rôle
              </li>
              <li className="flex items-center gap-3">
                <span className="text-green-500 text-lg">✅</span>
                Données sensibles masquées (dynamic data masking)
              </li>
              <li className="flex items-center gap-3">
                <span className="text-green-500 text-lg">✅</span>
                Orchestration multi-tâches avec dépendances
              </li>
              <li className="flex items-center gap-3">
                <span className="text-green-500 text-lg">✅</span>
                Monitoring, alertes et quality checks automatisés
              </li>
            </ul>
          </div>
        </section>

        {/* ══════════════════════════════════════════════════════════════
            InfoBoxes
        ══════════════════════════════════════════════════════════════ */}
        <section className="mb-12 space-y-4">
          <InfoBox type="tip" title="Certification">
            Ce projet couvre <strong>80% des questions</strong> de la
            certification <strong>Databricks Data Engineer Associate</strong>.
            Maîtrisez chaque phase et vous serez prêt pour l&apos;examen !
          </InfoBox>

          <InfoBox type="warning" title="Entretiens">
            Dans un entretien SNCF, montrez que vous comprenez les{" "}
            <strong>enjeux métier</strong> : ponctualité (objectif national
            de 90%), maintenance prédictive (réduction des coûts de 30%),
            satisfaction voyageur (NPS). Les recruteurs veulent voir que vous
            reliez la technique aux problématiques terrain.
          </InfoBox>
        </section>

        {/* ══════════════════════════════════════════════════════════════
            Récapitulatif
        ══════════════════════════════════════════════════════════════ */}
        <section className="mb-12">
          <div className="bg-gradient-to-br from-[#1b3a4b] to-[#2d5f7a] text-white rounded-2xl p-8">
            <h2 className="text-xl font-bold mb-4">📋 Récapitulatif du projet</h2>
            <div className="grid sm:grid-cols-3 gap-4">
              <div className="bg-white/10 rounded-lg p-4">
                <div className="text-2xl mb-2">🥉</div>
                <h3 className="font-bold mb-1">Bronze</h3>
                <p className="text-sm text-white/80">
                  6 tables brutes (trains, gares, trajets, retards,
                  maintenance, satisfaction) avec métadonnées Auto Loader.
                </p>
              </div>
              <div className="bg-white/10 rounded-lg p-4">
                <div className="text-2xl mb-2">🥈</div>
                <h3 className="font-bold mb-1">Silver</h3>
                <p className="text-sm text-white/80">
                  Données nettoyées, dédupliquées, enrichies par jointures.
                  Expectations DLT pour la qualité.
                </p>
              </div>
              <div className="bg-white/10 rounded-lg p-4">
                <div className="text-2xl mb-2">🥇</div>
                <h3 className="font-bold mb-1">Gold</h3>
                <p className="text-sm text-white/80">
                  3 tables d&apos;agrégation : ponctualité par ligne,
                  maintenance prédictive, satisfaction par axe.
                </p>
              </div>
            </div>
          </div>
        </section>

        {/* Navigation bas de page */}
        <div className="flex flex-wrap gap-4 justify-between items-center pt-8 border-t border-gray-200">
          <Link
            href="/exercices"
            className="inline-flex items-center gap-2 px-5 py-2.5 rounded-lg bg-gray-100 text-[#1b3a4b] font-semibold hover:bg-gray-200 transition-colors"
          >
            ← Tous les exercices
          </Link>
          <Link
            href="/exercices/quiz-certification"
            className="inline-flex items-center gap-2 px-5 py-2.5 rounded-lg bg-[#ff3621] text-white font-semibold hover:bg-[#e02e1a] transition-colors"
          >
            Quiz Certification →
          </Link>
        </div>
      </div>
    </div>
  );
}
