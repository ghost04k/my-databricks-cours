"use client";

import { useState } from "react";
import Link from "next/link";
import CodeBlock from "@/components/CodeBlock";
import InfoBox from "@/components/InfoBox";

/* ───────── composants internes ───────── */

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
        className="inline-flex items-center gap-2 px-4 py-2 bg-[#1b3a4b] text-white text-sm font-medium rounded-lg hover:bg-[#2d5f7a] transition-colors"
        aria-expanded={open}
        aria-controls={id}
      >
        {open ? "🙈 Masquer la réponse" : "👁️ Voir la réponse"}
      </button>
      {open && (
        <div
          id={id}
          className="mt-4 p-5 bg-gray-50 rounded-xl border border-gray-200"
        >
          {children}
        </div>
      )}
    </div>
  );
}

function ProgressTracker() {
  const [checked, setChecked] = useState<Record<string, boolean>>({});
  const toggle = (id: string) =>
    setChecked((prev) => ({ ...prev, [id]: !prev[id] }));

  const items = [
    { id: "cv", label: "CV mis à jour avec compétences Databricks" },
    { id: "linkedin", label: "Profil LinkedIn optimisé (Data Engineer)" },
    { id: "portfolio", label: "Portfolio GitHub avec projets Databricks" },
    { id: "cert", label: "Certification Databricks DE Associate passée" },
    { id: "practice", label: "20+ questions techniques maîtrisées" },
    { id: "project", label: "Projet final SNCF complété" },
    { id: "mock", label: "Au moins 2 entretiens blancs réalisés" },
  ];

  const done = Object.values(checked).filter(Boolean).length;

  return (
    <div className="bg-white rounded-xl border border-gray-200 p-6 mb-8">
      <h3 className="text-lg font-bold text-[#1b3a4b] mb-1">
        🎯 Checklist de préparation
      </h3>
      <p className="text-sm text-gray-500 mb-4">
        {done}/{items.length} complétés
      </p>
      <div className="w-full bg-gray-200 rounded-full h-2.5 mb-4">
        <div
          className="bg-[#00a972] h-2.5 rounded-full transition-all duration-300"
          style={{ width: `${(done / items.length) * 100}%` }}
        />
      </div>
      <div className="space-y-2">
        {items.map((item) => (
          <label
            key={item.id}
            className="flex items-center gap-3 cursor-pointer group"
          >
            <input
              type="checkbox"
              checked={!!checked[item.id]}
              onChange={() => toggle(item.id)}
              className="w-4 h-4 rounded accent-[#00a972]"
            />
            <span
              className={`text-sm ${
                checked[item.id]
                  ? "line-through text-gray-400"
                  : "text-gray-700 group-hover:text-[#1b3a4b]"
              }`}
            >
              {item.label}
            </span>
          </label>
        ))}
      </div>
    </div>
  );
}

/* ───────── page principale ───────── */

export default function PreparationEntretiensPage() {
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
            <span className="text-sm text-white/70">⏱ 5 heures</span>
          </div>
          <h1 className="text-3xl lg:text-4xl font-extrabold mb-3">
            🎯 Préparation aux Entretiens Data Engineer
          </h1>
          <p className="text-lg text-white/80 max-w-2xl leading-relaxed">
            Maximisez vos chances de décrocher un poste de Data Engineer dans
            les grandes entreprises françaises — SNCF, TotalEnergies, BNP
            Paribas, Société Générale et bien d&apos;autres.
          </p>
        </div>
      </div>

      {/* Content */}
      <div className="max-w-4xl mx-auto px-6 py-10 lg:px-10">
        {/* Breadcrumb */}
        <div className="flex flex-wrap gap-3 mb-10">
          <Link
            href="/exercices"
            className="inline-flex items-center gap-2 text-sm text-[#1b3a4b] hover:text-[#ff3621] transition-colors"
          >
            ← Exercices
          </Link>
          <span className="text-gray-300">|</span>
          <span className="text-sm text-gray-500">Préparation Entretiens</span>
        </div>

        {/* Progress Tracker */}
        <ProgressTracker />

        <InfoBox type="tip" title="Conseil clé">
          Les grandes entreprises cherchent des Data Engineers qui comprennent
          les <strong>enjeux métier</strong>, pas juste la technique. Montrez
          que vous comprenez pourquoi vous faites les choses, pas seulement
          comment.
        </InfoBox>

        {/* ══════════════════════════════════════════════════════════════
            SECTION 1 — Entreprises cibles
        ══════════════════════════════════════════════════════════════ */}
        <section className="mb-14">
          <h2 className="text-2xl font-bold text-[#1b3a4b] mb-4">
            🏢 Entreprises cibles et ce qu&apos;elles recherchent
          </h2>
          <p className="text-gray-700 leading-relaxed mb-6">
            Voici un panorama des principales entreprises françaises qui
            recrutent des Data Engineers Databricks, leur stack et ce
            qu&apos;elles attendent de vous.
          </p>

          <div className="bg-white rounded-xl border border-gray-200 p-5 mb-6 overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-gray-200">
                  <th className="text-left py-2 pr-4 font-semibold text-[#1b3a4b]">
                    Entreprise
                  </th>
                  <th className="text-left py-2 pr-4 font-semibold text-[#1b3a4b]">
                    Secteur
                  </th>
                  <th className="text-left py-2 pr-4 font-semibold text-[#1b3a4b]">
                    Stack Data
                  </th>
                  <th className="text-left py-2 font-semibold text-[#1b3a4b]">
                    Ce qu&apos;ils cherchent
                  </th>
                </tr>
              </thead>
              <tbody className="text-gray-700">
                <tr className="border-b border-gray-100">
                  <td className="py-2 pr-4 font-medium">🚄 SNCF</td>
                  <td className="py-2 pr-4">Transport</td>
                  <td className="py-2 pr-4">
                    <code className="bg-blue-50 text-blue-700 px-1.5 py-0.5 rounded text-xs">
                      Databricks / Azure
                    </code>
                  </td>
                  <td className="py-2">
                    Pipelines temps réel, IoT ferroviaire, maintenance
                    prédictive
                  </td>
                </tr>
                <tr className="border-b border-gray-100">
                  <td className="py-2 pr-4 font-medium">⚡ TotalEnergies</td>
                  <td className="py-2 pr-4">Énergie</td>
                  <td className="py-2 pr-4">
                    <code className="bg-green-50 text-green-700 px-1.5 py-0.5 rounded text-xs">
                      Databricks / AWS
                    </code>
                  </td>
                  <td className="py-2">
                    IoT + Data Lake, traitement capteurs, optimisation
                    énergétique
                  </td>
                </tr>
                <tr className="border-b border-gray-100">
                  <td className="py-2 pr-4 font-medium">🏦 BNP Paribas</td>
                  <td className="py-2 pr-4">Banque</td>
                  <td className="py-2 pr-4">
                    <code className="bg-purple-50 text-purple-700 px-1.5 py-0.5 rounded text-xs">
                      Databricks / GCP
                    </code>
                  </td>
                  <td className="py-2">
                    Conformité RGPD, agrégation risques, lineage des données
                  </td>
                </tr>
                <tr className="border-b border-gray-100">
                  <td className="py-2 pr-4 font-medium">
                    🏦 Société Générale
                  </td>
                  <td className="py-2 pr-4">Banque</td>
                  <td className="py-2 pr-4">
                    <code className="bg-blue-50 text-blue-700 px-1.5 py-0.5 rounded text-xs">
                      Databricks / Azure
                    </code>
                  </td>
                  <td className="py-2">
                    Risk analytics, data mesh, pipelines réglementaires
                  </td>
                </tr>
                <tr className="border-b border-gray-100">
                  <td className="py-2 pr-4 font-medium">📡 Orange</td>
                  <td className="py-2 pr-4">Télécom</td>
                  <td className="py-2 pr-4">
                    <code className="bg-green-50 text-green-700 px-1.5 py-0.5 rounded text-xs">
                      Databricks / AWS
                    </code>
                  </td>
                  <td className="py-2">
                    Big data client, analyse réseau, personnalisation offres
                  </td>
                </tr>
                <tr>
                  <td className="py-2 pr-4 font-medium">🛒 Carrefour</td>
                  <td className="py-2 pr-4">Retail</td>
                  <td className="py-2 pr-4">
                    <code className="bg-purple-50 text-purple-700 px-1.5 py-0.5 rounded text-xs">
                      Databricks / GCP
                    </code>
                  </td>
                  <td className="py-2">
                    Supply chain analytics, prévision demande, fidélité client
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
        </section>

        {/* ══════════════════════════════════════════════════════════════
            SECTION 2 — Questions techniques (20)
        ══════════════════════════════════════════════════════════════ */}
        <section className="mb-14">
          <h2 className="text-2xl font-bold text-[#1b3a4b] mb-4">
            💬 Questions techniques fréquentes
          </h2>
          <p className="text-gray-700 leading-relaxed mb-6">
            Les 20 questions les plus posées en entretien Data Engineer
            Databricks, classées par thème. Entraînez-vous à y répondre{" "}
            <strong>à voix haute</strong> pour gagner en fluidité.
          </p>

          <InfoBox type="important" title="Important">
            Pratiquez à voix haute ! Expliquer Delta Lake à un ami est le
            meilleur entraînement. Vous devez pouvoir répondre à chaque
            question en <strong>2-3 minutes</strong> maximum.
          </InfoBox>

          {/* ── A. Architecture & Delta Lake ── */}
          <h3 className="text-xl font-bold text-[#1b3a4b] mt-10 mb-6 flex items-center gap-2">
            <span className="inline-flex items-center justify-center w-8 h-8 rounded-full bg-blue-100 text-blue-700 text-sm font-bold">
              A
            </span>
            Architecture &amp; Delta Lake
          </h3>

          {/* Q1 */}
          <div className="mb-8 border-l-4 border-blue-300 pl-5">
            <p className="font-semibold text-[#1b3a4b]">
              1. Qu&apos;est-ce que l&apos;architecture Lakehouse et en quoi
              diffère-t-elle d&apos;un Data Warehouse classique ?
            </p>
            <SolutionToggle id="q1">
              <p className="text-sm text-gray-700 mb-3">
                <strong>Points clés à mentionner :</strong>
              </p>
              <ul className="list-disc list-inside text-sm text-gray-700 space-y-1 mb-4">
                <li>
                  <strong>Lakehouse = meilleur des deux mondes</strong> : la
                  flexibilité et le faible coût du Data Lake + la fiabilité et
                  les performances du Data Warehouse.
                </li>
                <li>
                  Repose sur <strong>Delta Lake</strong> qui apporte les
                  transactions ACID, le schema enforcement et le time travel
                  sur un stockage objet (S3, ADLS, GCS).
                </li>
                <li>
                  <strong>Data Warehouse classique</strong> : ne gère pas les
                  données non structurées, scaling coûteux, vendor lock-in.
                </li>
                <li>
                  <strong>Data Lake seul</strong> : pas de transactions ACID,
                  pas de schéma strict, problèmes de qualité (« data swamp »).
                </li>
                <li>
                  Le Lakehouse supporte BI, ML, streaming et batch sur la
                  <strong>même plateforme</strong>.
                </li>
              </ul>
              <p className="text-sm text-gray-500 italic">
                💡 Ce que le recruteur veut entendre : vous comprenez les
                limites des approches précédentes et pourquoi le Lakehouse
                résout ces problèmes.
              </p>
            </SolutionToggle>
          </div>

          {/* Q2 */}
          <div className="mb-8 border-l-4 border-blue-300 pl-5">
            <p className="font-semibold text-[#1b3a4b]">
              2. Expliquez le format Delta Lake et ses avantages.
            </p>
            <SolutionToggle id="q2">
              <p className="text-sm text-gray-700 mb-3">
                <strong>Points clés :</strong>
              </p>
              <ul className="list-disc list-inside text-sm text-gray-700 space-y-1 mb-4">
                <li>
                  Delta Lake = fichiers <strong>Parquet</strong> + un{" "}
                  <strong>transaction log</strong> (
                  <code className="bg-gray-100 px-1 rounded text-xs">
                    _delta_log/
                  </code>
                  )
                </li>
                <li>
                  <strong>ACID</strong> : atomicité, cohérence, isolation,
                  durabilité
                </li>
                <li>
                  <strong>Time Travel</strong> : accéder aux versions
                  précédentes des données
                </li>
                <li>
                  <strong>Schema Enforcement</strong> : rejette les écritures
                  qui ne respectent pas le schéma
                </li>
                <li>
                  <strong>Schema Evolution</strong> :{" "}
                  <code className="bg-gray-100 px-1 rounded text-xs">
                    mergeSchema
                  </code>{" "}
                  pour faire évoluer le schéma
                </li>
                <li>
                  <strong>OPTIMIZE + Z-ORDER</strong> : compaction et
                  co-localisation pour des requêtes rapides
                </li>
                <li>
                  <strong>VACUUM</strong> : nettoyage des fichiers obsolètes
                </li>
              </ul>
              <CodeBlock language="sql" code={`-- Exemples de commandes Delta Lake
DESCRIBE HISTORY my_table;           -- Historique des versions
SELECT * FROM my_table VERSION AS OF 3; -- Time Travel
OPTIMIZE my_table ZORDER BY (city);  -- Compaction + Z-ORDER
VACUUM my_table RETAIN 168 HOURS;    -- Nettoyage (7 jours)`} />
              <p className="text-sm text-gray-500 italic mt-3">
                💡 Ce que le recruteur veut entendre : vous maîtrisez le
                fonctionnement interne (transaction log) et savez quand
                utiliser chaque fonctionnalité.
              </p>
            </SolutionToggle>
          </div>

          {/* Q3 */}
          <div className="mb-8 border-l-4 border-blue-300 pl-5">
            <p className="font-semibold text-[#1b3a4b]">
              3. Quelle est la différence entre une table managed et une table
              external ?
            </p>
            <SolutionToggle id="q3">
              <p className="text-sm text-gray-700 mb-3">
                <strong>Points clés :</strong>
              </p>
              <ul className="list-disc list-inside text-sm text-gray-700 space-y-1 mb-4">
                <li>
                  <strong>Table managed (gérée)</strong> : Databricks gère les
                  métadonnées ET les données. Un{" "}
                  <code className="bg-gray-100 px-1 rounded text-xs">
                    DROP TABLE
                  </code>{" "}
                  supprime tout (métadonnées + fichiers).
                </li>
                <li>
                  <strong>Table external (externe)</strong> : Databricks gère
                  uniquement les métadonnées. Un{" "}
                  <code className="bg-gray-100 px-1 rounded text-xs">
                    DROP TABLE
                  </code>{" "}
                  supprime les métadonnées mais{" "}
                  <strong>garde les fichiers</strong> sur le stockage.
                </li>
                <li>
                  Utiliser{" "}
                  <code className="bg-gray-100 px-1 rounded text-xs">
                    DESCRIBE EXTENDED
                  </code>{" "}
                  pour vérifier le type et l&apos;emplacement.
                </li>
              </ul>
              <CodeBlock language="sql" code={`-- Table managed (pas de LOCATION)
CREATE TABLE catalog.schema.managed_table (
  id INT, name STRING
);

-- Table external (avec LOCATION)
CREATE TABLE catalog.schema.external_table (
  id INT, name STRING
)
LOCATION 's3://bucket/path/to/data';

-- Vérifier le type
DESCRIBE EXTENDED catalog.schema.managed_table;`} />
              <p className="text-sm text-gray-500 italic mt-3">
                💡 Ce que le recruteur veut entendre : vous savez quand
                utiliser l&apos;une ou l&apos;autre (managed pour les tables
                internes, external pour partager des données entre systèmes).
              </p>
            </SolutionToggle>
          </div>

          {/* Q4 */}
          <div className="mb-8 border-l-4 border-blue-300 pl-5">
            <p className="font-semibold text-[#1b3a4b]">
              4. Comment fonctionne le Time Travel dans Delta Lake ?
            </p>
            <SolutionToggle id="q4">
              <p className="text-sm text-gray-700 mb-3">
                <strong>Points clés :</strong>
              </p>
              <ul className="list-disc list-inside text-sm text-gray-700 space-y-1 mb-4">
                <li>
                  Chaque opération crée une nouvelle version dans le{" "}
                  <strong>_delta_log</strong> (fichiers JSON + checkpoints
                  Parquet).
                </li>
                <li>
                  On peut interroger n&apos;importe quelle version passée.
                </li>
                <li>
                  On peut <strong>restaurer</strong> une table à une version
                  antérieure.
                </li>
                <li>
                  <strong>VACUUM</strong> nettoie les anciens fichiers (défaut
                  : 7 jours de rétention).
                </li>
                <li>
                  Après un VACUUM, les versions antérieures à la période de
                  rétention ne sont plus accessibles.
                </li>
              </ul>
              <CodeBlock language="sql" code={`-- Accéder à une version spécifique
SELECT * FROM my_table VERSION AS OF 3;

-- Accéder par timestamp
SELECT * FROM my_table TIMESTAMP AS OF '2025-01-15T10:00:00';

-- Restaurer une version
RESTORE TABLE my_table TO VERSION AS OF 5;

-- Voir l'historique complet
DESCRIBE HISTORY my_table;

-- Nettoyer les anciens fichiers
VACUUM my_table RETAIN 168 HOURS; -- 7 jours`} />
              <p className="text-sm text-gray-500 italic mt-3">
                💡 Ce que le recruteur veut entendre : vous connaissez les cas
                d&apos;usage (audit, rollback, debug) et les limitations
                (VACUUM).
              </p>
            </SolutionToggle>
          </div>

          {/* Q5 */}
          <div className="mb-8 border-l-4 border-blue-300 pl-5">
            <p className="font-semibold text-[#1b3a4b]">
              5. Expliquez OPTIMIZE et Z-ORDER.
            </p>
            <SolutionToggle id="q5">
              <p className="text-sm text-gray-700 mb-3">
                <strong>Points clés :</strong>
              </p>
              <ul className="list-disc list-inside text-sm text-gray-700 space-y-1 mb-4">
                <li>
                  <strong>OPTIMIZE</strong> = compaction des petits fichiers en
                  fichiers plus grands (réduit le « small file problem »).
                </li>
                <li>
                  <strong>Z-ORDER</strong> = co-localise les données liées
                  dans les mêmes fichiers selon les colonnes spécifiées →
                  data skipping plus efficace.
                </li>
                <li>
                  Z-ORDER est utile pour les colonnes souvent utilisées en{" "}
                  <code className="bg-gray-100 px-1 rounded text-xs">
                    WHERE
                  </code>{" "}
                  ou{" "}
                  <code className="bg-gray-100 px-1 rounded text-xs">
                    JOIN
                  </code>
                  .
                </li>
                <li>
                  Limiter à <strong>2-4 colonnes</strong> pour Z-ORDER
                  (au-delà, l&apos;efficacité diminue).
                </li>
              </ul>
              <CodeBlock language="sql" code={`-- Compaction simple
OPTIMIZE my_table;

-- Compaction + Z-ORDER sur une colonne
OPTIMIZE my_table ZORDER BY (country);

-- Z-ORDER multi-colonnes
OPTIMIZE my_table ZORDER BY (country, city);

-- Vérifier la taille des fichiers
DESCRIBE DETAIL my_table;`} />
              <p className="text-sm text-gray-500 italic mt-3">
                💡 Ce que le recruteur veut entendre : vous savez quand et
                pourquoi utiliser ces commandes, et pas seulement la syntaxe.
              </p>
            </SolutionToggle>
          </div>

          {/* ── B. Streaming & Ingestion ── */}
          <h3 className="text-xl font-bold text-[#1b3a4b] mt-10 mb-6 flex items-center gap-2">
            <span className="inline-flex items-center justify-center w-8 h-8 rounded-full bg-green-100 text-green-700 text-sm font-bold">
              B
            </span>
            Streaming &amp; Ingestion
          </h3>

          {/* Q6 */}
          <div className="mb-8 border-l-4 border-green-300 pl-5">
            <p className="font-semibold text-[#1b3a4b]">
              6. Quelle différence entre Auto Loader et COPY INTO ?
            </p>
            <SolutionToggle id="q6">
              <p className="text-sm text-gray-700 mb-3">
                <strong>Points clés :</strong>
              </p>
              <ul className="list-disc list-inside text-sm text-gray-700 space-y-1 mb-4">
                <li>
                  <strong>Auto Loader</strong> (
                  <code className="bg-gray-100 px-1 rounded text-xs">
                    cloudFiles
                  </code>
                  ) : ingestion incrémentale via notifications cloud ou listing
                  de répertoire. Scalable, efficace pour des millions de
                  fichiers, détecte automatiquement les nouveaux fichiers.
                </li>
                <li>
                  <strong>COPY INTO</strong> : commande SQL idempotente, liste
                  les fichiers à chaque exécution. Plus simple mais moins
                  performant à grande échelle.
                </li>
                <li>
                  Auto Loader maintient un <strong>checkpoint</strong> pour
                  traquer les fichiers déjà ingérés.
                </li>
                <li>
                  Recommandation : <strong>Auto Loader</strong> pour la
                  production, COPY INTO pour des chargements ponctuels.
                </li>
              </ul>
              <CodeBlock language="python" code={`# Auto Loader (recommandé en production)
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/checkpoints/schema")
    .load("/data/landing/events/")
)

df.writeStream \\
    .option("checkpointLocation", "/checkpoints/bronze_events") \\
    .trigger(availableNow=True) \\
    .toTable("bronze.events")`} />
              <CodeBlock language="sql" code={`-- COPY INTO (chargement ponctuel)
COPY INTO bronze.events
FROM '/data/landing/events/'
FILEFORMAT = JSON
FORMAT_OPTIONS ('mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');`} />
              <p className="text-sm text-gray-500 italic mt-3">
                💡 Ce que le recruteur veut entendre : vous savez quand
                utiliser l&apos;un ou l&apos;autre selon le contexte
                (volume, fréquence, criticité).
              </p>
            </SolutionToggle>
          </div>

          {/* Q7 */}
          <div className="mb-8 border-l-4 border-green-300 pl-5">
            <p className="font-semibold text-[#1b3a4b]">
              7. Comment fonctionne Structured Streaming ?
            </p>
            <SolutionToggle id="q7">
              <p className="text-sm text-gray-700 mb-3">
                <strong>Points clés :</strong>
              </p>
              <ul className="list-disc list-inside text-sm text-gray-700 space-y-1 mb-4">
                <li>
                  Modèle de traitement «{" "}
                  <strong>micro-batch incrémental</strong> » : les nouvelles
                  données sont traitées comme un DataFrame en annexe.
                </li>
                <li>
                  <strong>Checkpoint</strong> : assure la tolérance aux pannes
                  et le traitement exactly-once.
                </li>
                <li>
                  <strong>Triggers</strong> :{" "}
                  <code className="bg-gray-100 px-1 rounded text-xs">
                    availableNow
                  </code>{" "}
                  (batch incrémental),{" "}
                  <code className="bg-gray-100 px-1 rounded text-xs">
                    processingTime
                  </code>{" "}
                  (intervalle fixe),{" "}
                  <code className="bg-gray-100 px-1 rounded text-xs">
                    continuous
                  </code>{" "}
                  (faible latence, expérimental).
                </li>
                <li>
                  <strong>Output modes</strong> : append (nouvelles lignes
                  uniquement), complete (résultat complet), update (lignes
                  modifiées).
                </li>
                <li>
                  Supporte les sources : Kafka, fichiers (Auto Loader), Delta
                  table, Rate.
                </li>
              </ul>
              <CodeBlock language="python" code={`# Structured Streaming avec trigger
stream = (spark.readStream
    .table("bronze.events")
    .writeStream
    .option("checkpointLocation", "/checkpoints/silver_events")
    .trigger(availableNow=True)  # ou processingTime="5 minutes"
    .outputMode("append")
    .toTable("silver.events_clean")
)`} />
              <p className="text-sm text-gray-500 italic mt-3">
                💡 Ce que le recruteur veut entendre : vous comprenez le
                modèle conceptuel (table en annexe infinie) et les garanties
                (exactly-once, fault tolerance).
              </p>
            </SolutionToggle>
          </div>

          {/* Q8 */}
          <div className="mb-8 border-l-4 border-green-300 pl-5">
            <p className="font-semibold text-[#1b3a4b]">
              8. Qu&apos;est-ce que l&apos;architecture Medallion (Multi-Hop) ?
            </p>
            <SolutionToggle id="q8">
              <p className="text-sm text-gray-700 mb-3">
                <strong>Points clés :</strong>
              </p>
              <ul className="list-disc list-inside text-sm text-gray-700 space-y-1 mb-4">
                <li>
                  Architecture en <strong>3 couches</strong> pour organiser
                  les données progressivement :
                </li>
                <li>
                  🥉 <strong>Bronze</strong> : données brutes, « as-is »,
                  ajout de métadonnées d&apos;ingestion (source, date).
                </li>
                <li>
                  🥈 <strong>Silver</strong> : données nettoyées,
                  dédupliquées, validées, enrichies par jointures.
                </li>
                <li>
                  🥇 <strong>Gold</strong> : agrégations métier prêtes pour
                  la BI, les dashboards et le reporting.
                </li>
                <li>
                  Chaque couche est une <strong>table Delta</strong>,
                  permettant le time travel et la traçabilité.
                </li>
              </ul>
              <CodeBlock language="sql" code={`-- Bronze : ingestion brute
CREATE OR REFRESH STREAMING TABLE bronze_orders
AS SELECT *, current_timestamp() AS ingested_at, input_file_name() AS source_file
FROM cloud_files('/data/orders/', 'json');

-- Silver : nettoyage
CREATE OR REFRESH STREAMING TABLE silver_orders (
  CONSTRAINT valid_amount EXPECT (amount > 0) ON VIOLATION DROP ROW
)
AS SELECT *, CAST(order_date AS DATE) AS order_date_clean
FROM STREAM(LIVE.bronze_orders)
WHERE order_id IS NOT NULL;

-- Gold : agrégation métier
CREATE OR REFRESH LIVE TABLE gold_daily_revenue
AS SELECT order_date_clean, SUM(amount) AS total_revenue, COUNT(*) AS nb_orders
FROM LIVE.silver_orders
GROUP BY order_date_clean;`} />
              <p className="text-sm text-gray-500 italic mt-3">
                💡 Ce que le recruteur veut entendre : vous pouvez dessiner
                l&apos;architecture au tableau et expliquer la valeur ajoutée
                de chaque couche.
              </p>
            </SolutionToggle>
          </div>

          {/* Q9 */}
          <div className="mb-8 border-l-4 border-green-300 pl-5">
            <p className="font-semibold text-[#1b3a4b]">
              9. Comment gérer l&apos;évolution de schéma ?
            </p>
            <SolutionToggle id="q9">
              <p className="text-sm text-gray-700 mb-3">
                <strong>Points clés :</strong>
              </p>
              <ul className="list-disc list-inside text-sm text-gray-700 space-y-1 mb-4">
                <li>
                  <strong>Schema Enforcement</strong> (par défaut) : rejette
                  les écritures qui ne correspondent pas au schéma existant →
                  protège la qualité.
                </li>
                <li>
                  <strong>Schema Evolution</strong> : permet d&apos;ajouter
                  de nouvelles colonnes automatiquement avec{" "}
                  <code className="bg-gray-100 px-1 rounded text-xs">
                    mergeSchema
                  </code>
                  .
                </li>
                <li>
                  Auto Loader peut détecter et gérer l&apos;évolution de
                  schéma automatiquement via{" "}
                  <code className="bg-gray-100 px-1 rounded text-xs">
                    cloudFiles.schemaEvolutionMode
                  </code>
                  .
                </li>
                <li>
                  En production, privilégier un contrôle explicite du schéma
                  (audit des changements, alertes).
                </li>
              </ul>
              <CodeBlock language="python" code={`# Schema Evolution : ajout de colonnes
df.write \\
    .format("delta") \\
    .mode("append") \\
    .option("mergeSchema", "true") \\
    .saveAsTable("silver.events")

# Auto Loader avec schema evolution
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.schemaLocation", "/checkpoints/schema")
    .load("/data/events/")
)`} />
              <p className="text-sm text-gray-500 italic mt-3">
                💡 Ce que le recruteur veut entendre : vous connaissez la
                différence entre enforcement et evolution, et savez quand
                autoriser l&apos;un ou l&apos;autre.
              </p>
            </SolutionToggle>
          </div>

          {/* ── C. Production & Pipelines ── */}
          <h3 className="text-xl font-bold text-[#1b3a4b] mt-10 mb-6 flex items-center gap-2">
            <span className="inline-flex items-center justify-center w-8 h-8 rounded-full bg-orange-100 text-orange-700 text-sm font-bold">
              C
            </span>
            Production &amp; Pipelines
          </h3>

          {/* Q10 */}
          <div className="mb-8 border-l-4 border-orange-300 pl-5">
            <p className="font-semibold text-[#1b3a4b]">
              10. Qu&apos;est-ce que Delta Live Tables et pourquoi les
              utiliser ?
            </p>
            <SolutionToggle id="q10">
              <p className="text-sm text-gray-700 mb-3">
                <strong>Points clés :</strong>
              </p>
              <ul className="list-disc list-inside text-sm text-gray-700 space-y-1 mb-4">
                <li>
                  <strong>DLT</strong> = framework déclaratif pour créer des
                  pipelines de données fiables et maintenables.
                </li>
                <li>
                  Vous déclarez le <strong>résultat souhaité</strong> (quoi),
                  DLT gère l&apos;orchestration (comment).
                </li>
                <li>
                  Gestion automatique des <strong>dépendances</strong> entre
                  tables (DAG).
                </li>
                <li>
                  <strong>Expectations</strong> : contrôles de qualité
                  intégrés avec actions (warn, drop, fail).
                </li>
                <li>
                  Monitoring intégré via l&apos;interface DLT (event log,
                  data quality dashboard).
                </li>
              </ul>
              <CodeBlock language="sql" code={`-- Pipeline DLT déclaratif
CREATE OR REFRESH STREAMING TABLE bronze_events
AS SELECT * FROM cloud_files('/data/events/', 'json');

CREATE OR REFRESH STREAMING TABLE silver_events (
  CONSTRAINT valid_email EXPECT (email IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_timestamp EXPECT (event_ts > '2020-01-01') ON VIOLATION WARN
)
AS SELECT * FROM STREAM(LIVE.bronze_events)
WHERE event_type IS NOT NULL;

CREATE OR REFRESH LIVE TABLE gold_daily_stats
AS SELECT DATE(event_ts) AS event_date, COUNT(*) AS total
FROM LIVE.silver_events
GROUP BY DATE(event_ts);`} />
              <p className="text-sm text-gray-500 italic mt-3">
                💡 Ce que le recruteur veut entendre : DLT simplifie la
                maintenance et fiabilise les pipelines en production grâce à
                la déclaration et aux expectations.
              </p>
            </SolutionToggle>
          </div>

          {/* Q11 */}
          <div className="mb-8 border-l-4 border-orange-300 pl-5">
            <p className="font-semibold text-[#1b3a4b]">
              11. Expliquez les Expectations dans DLT.
            </p>
            <SolutionToggle id="q11">
              <p className="text-sm text-gray-700 mb-3">
                <strong>Points clés :</strong>
              </p>
              <ul className="list-disc list-inside text-sm text-gray-700 space-y-1 mb-4">
                <li>
                  Contraintes de qualité déclarées directement dans la
                  définition de la table.
                </li>
                <li>
                  <strong>3 types d&apos;actions</strong> :
                </li>
                <li>
                  <code className="bg-gray-100 px-1 rounded text-xs">
                    ON VIOLATION WARN
                  </code>{" "}
                  — enregistre la violation mais garde la ligne (monitoring).
                </li>
                <li>
                  <code className="bg-gray-100 px-1 rounded text-xs">
                    ON VIOLATION DROP ROW
                  </code>{" "}
                  — supprime la ligne invalide (nettoyage).
                </li>
                <li>
                  <code className="bg-gray-100 px-1 rounded text-xs">
                    ON VIOLATION FAIL
                  </code>{" "}
                  — arrête le pipeline (données critiques).
                </li>
                <li>
                  Les métriques de qualité sont visibles dans le{" "}
                  <strong>Event Log</strong> DLT.
                </li>
              </ul>
              <CodeBlock language="sql" code={`CREATE OR REFRESH STREAMING TABLE silver_transactions (
  -- Warning : on logue mais on garde
  CONSTRAINT valid_amount
    EXPECT (amount > 0)
    ON VIOLATION WARN,

  -- Drop : on supprime les lignes invalides
  CONSTRAINT valid_customer
    EXPECT (customer_id IS NOT NULL)
    ON VIOLATION DROP ROW,

  -- Fail : on arrête tout si données critiques corrompues
  CONSTRAINT valid_currency
    EXPECT (currency IN ('EUR', 'USD', 'GBP'))
    ON VIOLATION FAIL
)
AS SELECT * FROM STREAM(LIVE.bronze_transactions);`} />
              <p className="text-sm text-gray-500 italic mt-3">
                💡 Ce que le recruteur veut entendre : vous savez adapter le
                niveau d&apos;action à la criticité de la donnée.
              </p>
            </SolutionToggle>
          </div>

          {/* Q12 */}
          <div className="mb-8 border-l-4 border-orange-300 pl-5">
            <p className="font-semibold text-[#1b3a4b]">
              12. Comment orchestrer un pipeline de données ?
            </p>
            <SolutionToggle id="q12">
              <p className="text-sm text-gray-700 mb-3">
                <strong>Points clés :</strong>
              </p>
              <ul className="list-disc list-inside text-sm text-gray-700 space-y-1 mb-4">
                <li>
                  <strong>Databricks Workflows</strong> : orchestrateur natif
                  (tâches, dépendances, planning CRON, alertes).
                </li>
                <li>
                  Support de <strong>Tasks</strong> multiples : Notebook,
                  DLT Pipeline, Python, JAR, SQL.
                </li>
                <li>
                  Gestion des <strong>dépendances</strong> entre tâches (DAG).
                </li>
                <li>
                  <strong>Retry</strong> automatique avec backoff
                  configurable.
                </li>
                <li>
                  Intégration avec <strong>Airflow</strong>,{" "}
                  <strong>Azure Data Factory</strong>, etc.
                </li>
                <li>
                  <strong>Paramètres</strong> : passage de paramètres entre
                  tâches via{" "}
                  <code className="bg-gray-100 px-1 rounded text-xs">
                    dbutils.jobs.taskValues
                  </code>
                  .
                </li>
              </ul>
              <CodeBlock language="python" code={`# Passer un paramètre entre tâches
# Task 1 : producteur
dbutils.jobs.taskValues.set(key="row_count", value=df.count())

# Task 2 : consommateur
row_count = dbutils.jobs.taskValues.get(
    taskKey="task_1",
    key="row_count"
)
print(f"Task 1 a traité {row_count} lignes")`} />
              <p className="text-sm text-gray-500 italic mt-3">
                💡 Ce que le recruteur veut entendre : vous connaissez
                l&apos;orchestrateur natif ET les alternatives (Airflow), et
                savez quand utiliser chacun.
              </p>
            </SolutionToggle>
          </div>

          {/* Q13 */}
          <div className="mb-8 border-l-4 border-orange-300 pl-5">
            <p className="font-semibold text-[#1b3a4b]">
              13. Comment monitorer un pipeline en production ?
            </p>
            <SolutionToggle id="q13">
              <p className="text-sm text-gray-700 mb-3">
                <strong>Points clés :</strong>
              </p>
              <ul className="list-disc list-inside text-sm text-gray-700 space-y-1 mb-4">
                <li>
                  <strong>Spark UI</strong> : stages, tasks, shuffle, GC,
                  memory usage.
                </li>
                <li>
                  <strong>DLT Event Log</strong> : suivi des expectations,
                  métriques de qualité, durée des étapes.
                </li>
                <li>
                  <strong>Alertes Workflows</strong> : email/Slack/PagerDuty
                  sur échec, durée excessive, SLA breach.
                </li>
                <li>
                  <strong>Métriques personnalisées</strong> : row count,
                  data freshness, schema drift detection.
                </li>
                <li>
                  <strong>Logging structuré</strong> : utiliser{" "}
                  <code className="bg-gray-100 px-1 rounded text-xs">
                    log4j
                  </code>{" "}
                  ou la bibliothèque Python{" "}
                  <code className="bg-gray-100 px-1 rounded text-xs">
                    logging
                  </code>
                  .
                </li>
              </ul>
              <CodeBlock language="python" code={`# Monitoring basique : vérification post-pipeline
from datetime import datetime, timedelta

def check_data_freshness(table_name, max_delay_hours=2):
    """Vérifie que les données sont fraîches"""
    latest = spark.sql(f"""
        SELECT MAX(ingested_at) AS last_update
        FROM {table_name}
    """).first()["last_update"]

    if datetime.now() - latest > timedelta(hours=max_delay_hours):
        raise Alert(f"⚠️ {table_name} : données périmées !")

def check_row_count(table_name, min_expected=1000):
    """Vérifie le volume minimal"""
    count = spark.table(table_name).count()
    if count < min_expected:
        raise Alert(f"⚠️ {table_name} : seulement {count} lignes")`} />
              <p className="text-sm text-gray-500 italic mt-3">
                💡 Ce que le recruteur veut entendre : vous avez une approche
                proactive du monitoring (pas uniquement réactive sur les
                erreurs).
              </p>
            </SolutionToggle>
          </div>

          {/* ── D. Gouvernance ── */}
          <h3 className="text-xl font-bold text-[#1b3a4b] mt-10 mb-6 flex items-center gap-2">
            <span className="inline-flex items-center justify-center w-8 h-8 rounded-full bg-purple-100 text-purple-700 text-sm font-bold">
              D
            </span>
            Gouvernance
          </h3>

          {/* Q14 */}
          <div className="mb-8 border-l-4 border-purple-300 pl-5">
            <p className="font-semibold text-[#1b3a4b]">
              14. Qu&apos;est-ce que Unity Catalog ?
            </p>
            <SolutionToggle id="q14">
              <p className="text-sm text-gray-700 mb-3">
                <strong>Points clés :</strong>
              </p>
              <ul className="list-disc list-inside text-sm text-gray-700 space-y-1 mb-4">
                <li>
                  Solution de <strong>gouvernance unifiée</strong> pour
                  Databricks : métadonnées, permissions, audit, lineage.
                </li>
                <li>
                  Hiérarchie à <strong>3 niveaux</strong> :{" "}
                  <code className="bg-gray-100 px-1 rounded text-xs">
                    catalog.schema.table
                  </code>
                  .
                </li>
                <li>
                  <strong>Permissions granulaires</strong> : au niveau
                  catalog, schema, table, vue.
                </li>
                <li>
                  <strong>Data Lineage</strong> : traçabilité automatique des
                  transformations.
                </li>
                <li>
                  <strong>Audit</strong> : qui a accédé à quoi, quand.
                </li>
                <li>
                  Partage de données sécurisé via{" "}
                  <strong>Delta Sharing</strong>.
                </li>
              </ul>
              <CodeBlock language="sql" code={`-- Hiérarchie Unity Catalog
CREATE CATALOG IF NOT EXISTS production;
CREATE SCHEMA IF NOT EXISTS production.finance;
CREATE TABLE production.finance.transactions (...);

-- Permissions
GRANT USAGE ON CATALOG production TO data_team;
GRANT USAGE ON SCHEMA production.finance TO data_team;
GRANT SELECT ON TABLE production.finance.transactions TO data_analysts;

-- Vérifier les permissions
SHOW GRANTS ON TABLE production.finance.transactions;`} />
              <p className="text-sm text-gray-500 italic mt-3">
                💡 Ce que le recruteur veut entendre : vous comprenez que UC
                n&apos;est pas juste un catalogue, c&apos;est un système de
                gouvernance complet.
              </p>
            </SolutionToggle>
          </div>

          {/* Q15 */}
          <div className="mb-8 border-l-4 border-purple-300 pl-5">
            <p className="font-semibold text-[#1b3a4b]">
              15. Comment gérer la sécurité au niveau ligne/colonne ?
            </p>
            <SolutionToggle id="q15">
              <p className="text-sm text-gray-700 mb-3">
                <strong>Points clés :</strong>
              </p>
              <ul className="list-disc list-inside text-sm text-gray-700 space-y-1 mb-4">
                <li>
                  <strong>Row-level security</strong> : utiliser des{" "}
                  <strong>Dynamic Views</strong> qui filtrent les lignes
                  selon l&apos;utilisateur connecté.
                </li>
                <li>
                  <strong>Column-level security</strong> : masquer ou hasher
                  des colonnes sensibles dans des vues.
                </li>
                <li>
                  Fonctions utiles :{" "}
                  <code className="bg-gray-100 px-1 rounded text-xs">
                    current_user()
                  </code>
                  ,{" "}
                  <code className="bg-gray-100 px-1 rounded text-xs">
                    is_member()
                  </code>
                  .
                </li>
                <li>
                  Essentiel pour la conformité <strong>RGPD</strong> (données
                  personnelles).
                </li>
              </ul>
              <CodeBlock language="sql" code={`-- Row-level security : chaque manager voit ses équipes
CREATE VIEW secure_employees AS
SELECT * FROM employees
WHERE department = CASE
  WHEN is_member('admin') THEN department  -- admin voit tout
  ELSE current_user_department()           -- filtré par département
END;

-- Column masking : masquer les données sensibles
CREATE VIEW masked_customers AS
SELECT
  customer_id,
  CASE
    WHEN is_member('data_steward') THEN email
    ELSE regexp_replace(email, '(.).*@', '***@')
  END AS email,
  CASE
    WHEN is_member('data_steward') THEN phone
    ELSE 'XXX-XXX-' || RIGHT(phone, 4)
  END AS phone
FROM customers;`} />
              <p className="text-sm text-gray-500 italic mt-3">
                💡 Ce que le recruteur veut entendre : vous avez une
                sensibilité RGPD et savez protéger les données personnelles.
              </p>
            </SolutionToggle>
          </div>

          {/* Q16 */}
          <div className="mb-8 border-l-4 border-purple-300 pl-5">
            <p className="font-semibold text-[#1b3a4b]">
              16. Expliquez le principe du moindre privilège dans Unity
              Catalog.
            </p>
            <SolutionToggle id="q16">
              <p className="text-sm text-gray-700 mb-3">
                <strong>Points clés :</strong>
              </p>
              <ul className="list-disc list-inside text-sm text-gray-700 space-y-1 mb-4">
                <li>
                  Chaque utilisateur ou groupe ne reçoit que les permissions{" "}
                  <strong>strictement nécessaires</strong> à son travail.
                </li>
                <li>
                  Par défaut, <strong>aucun accès</strong> — on ajoute
                  explicitement les droits.
                </li>
                <li>
                  Utiliser des <strong>groupes</strong> plutôt que des
                  permissions individuelles.
                </li>
                <li>
                  Séparer les rôles :{" "}
                  <code className="bg-gray-100 px-1 rounded text-xs">
                    data_engineers
                  </code>{" "}
                  (CRUD),{" "}
                  <code className="bg-gray-100 px-1 rounded text-xs">
                    data_analysts
                  </code>{" "}
                  (SELECT),{" "}
                  <code className="bg-gray-100 px-1 rounded text-xs">
                    data_stewards
                  </code>{" "}
                  (GRANT).
                </li>
                <li>
                  Auditer régulièrement les permissions avec{" "}
                  <code className="bg-gray-100 px-1 rounded text-xs">
                    SHOW GRANTS
                  </code>
                  .
                </li>
              </ul>
              <CodeBlock language="sql" code={`-- Stratégie de moindre privilège
-- 1. Créer des groupes par rôle
-- (dans la console Admin Databricks)

-- 2. Permissions par couche
-- Data Engineers : accès complet Bronze/Silver, lecture Gold
GRANT ALL PRIVILEGES ON SCHEMA prod.bronze TO data_engineers;
GRANT ALL PRIVILEGES ON SCHEMA prod.silver TO data_engineers;
GRANT USAGE ON SCHEMA prod.gold TO data_engineers;
GRANT SELECT ON SCHEMA prod.gold TO data_engineers;

-- Data Analysts : lecture seule Gold
GRANT USAGE ON CATALOG prod TO data_analysts;
GRANT USAGE ON SCHEMA prod.gold TO data_analysts;
GRANT SELECT ON SCHEMA prod.gold TO data_analysts;

-- Data Scientists : lecture Silver + Gold
GRANT USAGE ON CATALOG prod TO data_scientists;
GRANT USAGE ON SCHEMA prod.silver TO data_scientists;
GRANT SELECT ON SCHEMA prod.silver TO data_scientists;
GRANT USAGE ON SCHEMA prod.gold TO data_scientists;
GRANT SELECT ON SCHEMA prod.gold TO data_scientists;

-- 3. Auditer
SHOW GRANTS ON SCHEMA prod.gold;`} />
              <p className="text-sm text-gray-500 italic mt-3">
                💡 Ce que le recruteur veut entendre : vous organisez les
                permissions de manière structurée, par groupes et par couches,
                avec une approche « deny by default ».
              </p>
            </SolutionToggle>
          </div>

          {/* ── E. Questions comportementales ── */}
          <h3 className="text-xl font-bold text-[#1b3a4b] mt-10 mb-6 flex items-center gap-2">
            <span className="inline-flex items-center justify-center w-8 h-8 rounded-full bg-red-100 text-red-700 text-sm font-bold">
              E
            </span>
            Questions comportementales / situationnelles
          </h3>

          {/* Q17 */}
          <div className="mb-8 border-l-4 border-red-300 pl-5">
            <p className="font-semibold text-[#1b3a4b]">
              17. Un pipeline échoue en production à 3h du matin. Que
              faites-vous ?
            </p>
            <SolutionToggle id="q17">
              <p className="text-sm text-gray-700 mb-3">
                <strong>Structure STAR :</strong>
              </p>
              <ul className="list-disc list-inside text-sm text-gray-700 space-y-1 mb-4">
                <li>
                  <strong>1. Triage immédiat</strong> : consulter l&apos;alerte
                  (email/Slack/PagerDuty), identifier le pipeline et la tâche
                  en erreur.
                </li>
                <li>
                  <strong>2. Diagnostic</strong> : lire les logs du driver,
                  vérifier le Spark UI, chercher l&apos;exception racine.
                </li>
                <li>
                  <strong>3. Causes fréquentes</strong> : schema drift (nouveau
                  champ), fichier source manquant/corrompu, cluster sous-dimensionné
                  (OOM), permissions révoquées.
                </li>
                <li>
                  <strong>4. Action</strong> : corriger le problème, relancer
                  le pipeline, vérifier les données résultantes.
                </li>
                <li>
                  <strong>5. Post-mortem</strong> : documenter l&apos;incident,
                  ajouter un test/expectation pour prévenir la récurrence.
                </li>
              </ul>
              <p className="text-sm text-gray-500 italic">
                💡 Ce que le recruteur veut entendre : méthodologie structurée,
                pas de panique, et une culture de l&apos;amélioration continue.
              </p>
            </SolutionToggle>
          </div>

          {/* Q18 */}
          <div className="mb-8 border-l-4 border-red-300 pl-5">
            <p className="font-semibold text-[#1b3a4b]">
              18. Comment gérez-vous un fichier source dont le schéma change
              sans prévenir ?
            </p>
            <SolutionToggle id="q18">
              <p className="text-sm text-gray-700 mb-3">
                <strong>Approche recommandée :</strong>
              </p>
              <ul className="list-disc list-inside text-sm text-gray-700 space-y-1 mb-4">
                <li>
                  <strong>Court terme</strong> : Auto Loader avec{" "}
                  <code className="bg-gray-100 px-1 rounded text-xs">
                    schemaEvolutionMode = addNewColumns
                  </code>{" "}
                  pour absorber les nouveaux champs en Bronze.
                </li>
                <li>
                  <strong>Schema Enforcement</strong> en Silver pour protéger
                  les consommateurs downstream.
                </li>
                <li>
                  Mettre en place une <strong>alerte de schema drift</strong>{" "}
                  pour être prévenu quand le schéma source change.
                </li>
                <li>
                  <strong>Documentation</strong> : contrat de données (data
                  contract) avec l&apos;équipe source.
                </li>
                <li>
                  <strong>Rescue column</strong> : Auto Loader peut stocker
                  les champs non reconnus dans{" "}
                  <code className="bg-gray-100 px-1 rounded text-xs">
                    _rescued_data
                  </code>
                  .
                </li>
              </ul>
              <CodeBlock language="python" code={`# Auto Loader avec rescued column pour gérer le drift
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/checkpoints/schema")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("rescuedDataColumn", "_rescued_data")
    .load("/data/source/")
)

# Alerter si des données sont "rescued" (schema drift détecté)
rescued = df.filter("_rescued_data IS NOT NULL").count()
if rescued > 0:
    send_alert(f"⚠️ Schema drift détecté : {rescued} lignes rescued")`} />
              <p className="text-sm text-gray-500 italic mt-3">
                💡 Ce que le recruteur veut entendre : approche défensive mais
                réaliste — le schéma va évoluer, il faut s&apos;y préparer.
              </p>
            </SolutionToggle>
          </div>

          {/* Q19 */}
          <div className="mb-8 border-l-4 border-red-300 pl-5">
            <p className="font-semibold text-[#1b3a4b]">
              19. Un data analyst se plaint que sa requête est lente. Comment
              debuggez-vous ?
            </p>
            <SolutionToggle id="q19">
              <p className="text-sm text-gray-700 mb-3">
                <strong>Checklist de diagnostic :</strong>
              </p>
              <ul className="list-disc list-inside text-sm text-gray-700 space-y-1 mb-4">
                <li>
                  <strong>1. Comprendre la requête</strong> : lire le SQL,
                  identifier les tables, les jointures, les filtres.
                </li>
                <li>
                  <strong>2. EXPLAIN</strong> : analyser le plan d&apos;exécution
                  (scan complet ? shuffle excessif ?).
                </li>
                <li>
                  <strong>3. Small file problem</strong> : la table a-t-elle
                  besoin d&apos;un{" "}
                  <code className="bg-gray-100 px-1 rounded text-xs">
                    OPTIMIZE
                  </code>
                  ?
                </li>
                <li>
                  <strong>4. Z-ORDER</strong> : les colonnes filtrées sont-elles
                  z-ordonnées ?
                </li>
                <li>
                  <strong>5. Cluster sizing</strong> : le cluster est-il
                  suffisant ? (Spark UI → stages, spill to disk).
                </li>
                <li>
                  <strong>6. Caching</strong> : si la requête est répétée,
                  créer une table Gold pré-agrégée.
                </li>
                <li>
                  <strong>7. Partitionnement</strong> : vérifier si la table
                  est partitionnée de manière pertinente.
                </li>
              </ul>
              <CodeBlock language="sql" code={`-- Diagnostic performance
EXPLAIN EXTENDED SELECT * FROM sales WHERE country = 'FR';

-- Vérifier la taille des fichiers
DESCRIBE DETAIL sales;

-- Optimiser
OPTIMIZE sales ZORDER BY (country, sale_date);

-- Vérifier les statistiques
ANALYZE TABLE sales COMPUTE STATISTICS FOR ALL COLUMNS;`} />
              <p className="text-sm text-gray-500 italic mt-3">
                💡 Ce que le recruteur veut entendre : approche méthodique (pas
                de « j&apos;ajoute plus de RAM »), compréhension des causes
                racines.
              </p>
            </SolutionToggle>
          </div>

          {/* Q20 */}
          <div className="mb-8 border-l-4 border-red-300 pl-5">
            <p className="font-semibold text-[#1b3a4b]">
              20. Comment assurez-vous la qualité des données dans vos
              pipelines ?
            </p>
            <SolutionToggle id="q20">
              <p className="text-sm text-gray-700 mb-3">
                <strong>Stratégie multi-couches :</strong>
              </p>
              <ul className="list-disc list-inside text-sm text-gray-700 space-y-1 mb-4">
                <li>
                  <strong>Bronze</strong> : Schema enforcement, rescued column
                  pour détecter le drift, métadonnées d&apos;ingestion.
                </li>
                <li>
                  <strong>Silver</strong> : DLT Expectations (nulls, ranges,
                  formats), déduplication, validation des jointures.
                </li>
                <li>
                  <strong>Gold</strong> : Tests d&apos;agrégation (row count,
                  sommes de contrôle), comparaison avec les résultats attendus.
                </li>
                <li>
                  <strong>Monitoring continu</strong> : data freshness, volume
                  anomaly detection, alertes automatiques.
                </li>
                <li>
                  <strong>Documentation</strong> : data dictionary, SLA sur
                  la fraîcheur, data contracts.
                </li>
              </ul>
              <CodeBlock language="sql" code={`-- Expectations DLT multi-niveaux
CREATE OR REFRESH STREAMING TABLE silver_orders (
  -- Données critiques : stop le pipeline
  CONSTRAINT pk_not_null
    EXPECT (order_id IS NOT NULL)
    ON VIOLATION FAIL,

  -- Qualité importante : supprimer les lignes invalides
  CONSTRAINT valid_amount
    EXPECT (amount > 0 AND amount < 1000000)
    ON VIOLATION DROP ROW,

  -- Monitoring : garder mais logger
  CONSTRAINT valid_email
    EXPECT (email RLIKE '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$')
    ON VIOLATION WARN
)
AS SELECT * FROM STREAM(LIVE.bronze_orders);`} />
              <p className="text-sm text-gray-500 italic mt-3">
                💡 Ce que le recruteur veut entendre : approche « defense in
                depth » — plusieurs niveaux de contrôle, pas un seul.
              </p>
            </SolutionToggle>
          </div>
        </section>

        {/* ══════════════════════════════════════════════════════════════
            SECTION 3 — Portfolio
        ══════════════════════════════════════════════════════════════ */}
        <section className="mb-14">
          <h2 className="text-2xl font-bold text-[#1b3a4b] mb-4">
            📝 Construire son portfolio
          </h2>
          <p className="text-gray-700 leading-relaxed mb-6">
            Un portfolio GitHub solide est votre meilleur atout pour décrocher
            un entretien. Voici comment le structurer.
          </p>

          <div className="bg-white rounded-xl border border-gray-200 p-6 mb-6">
            <h3 className="text-lg font-bold text-[#1b3a4b] mb-3">
              🧑‍💻 Profil GitHub à soigner
            </h3>
            <ul className="list-disc list-inside text-sm text-gray-700 space-y-2">
              <li>
                Photo professionnelle et bio claire : « Data Engineer | Databricks | Delta Lake | Python/SQL »
              </li>
              <li>
                README de profil avec un résumé de vos compétences et projets
              </li>
              <li>
                Repositories bien organisés avec des README détaillés
              </li>
              <li>
                Contribution régulière (commits fréquents, pas tout d&apos;un
                coup)
              </li>
            </ul>
          </div>

          <div className="bg-white rounded-xl border border-gray-200 p-6 mb-6">
            <h3 className="text-lg font-bold text-[#1b3a4b] mb-3">
              📂 Projets à mettre en avant
            </h3>
            <div className="grid gap-4 sm:grid-cols-2">
              <div className="bg-gray-50 rounded-lg p-4 border border-gray-100">
                <div className="text-xl mb-2">🥉🥈🥇</div>
                <h4 className="font-semibold text-[#1b3a4b] text-sm mb-1">
                  1. Pipeline Medallion
                </h4>
                <p className="text-xs text-gray-600">
                  Architecture Bronze → Silver → Gold complète avec Delta
                  Lake. Ingestion, nettoyage, agrégation.
                </p>
              </div>
              <div className="bg-gray-50 rounded-lg p-4 border border-gray-100">
                <div className="text-xl mb-2">⚡</div>
                <h4 className="font-semibold text-[#1b3a4b] text-sm mb-1">
                  2. Streaming avec Auto Loader
                </h4>
                <p className="text-xs text-gray-600">
                  Pipeline temps réel avec Auto Loader, schema evolution et
                  checkpointing.
                </p>
              </div>
              <div className="bg-gray-50 rounded-lg p-4 border border-gray-100">
                <div className="text-xl mb-2">✅</div>
                <h4 className="font-semibold text-[#1b3a4b] text-sm mb-1">
                  3. DLT avec quality expectations
                </h4>
                <p className="text-xs text-gray-600">
                  Pipeline DLT déclaratif avec expectations multi-niveaux
                  (warn, drop, fail).
                </p>
              </div>
              <div className="bg-gray-50 rounded-lg p-4 border border-gray-100">
                <div className="text-xl mb-2">🔒</div>
                <h4 className="font-semibold text-[#1b3a4b] text-sm mb-1">
                  4. Gouvernance Unity Catalog
                </h4>
                <p className="text-xs text-gray-600">
                  Setup complet avec permissions granulaires, row-level
                  security et audit.
                </p>
              </div>
            </div>
          </div>

          <div className="mb-6">
            <h3 className="text-lg font-bold text-[#1b3a4b] mb-3">
              📄 Structure de README recommandée
            </h3>
            <p className="text-sm text-gray-700 mb-3">
              Un bon README doit répondre aux questions : <strong>Quoi ?
              Pourquoi ? Comment ?</strong>
            </p>
            <CodeBlock language="markdown" code={`# 🚀 Pipeline Medallion - E-commerce Analytics

## 📋 Description
Pipeline de données complet pour analyser les commandes e-commerce
en temps réel, utilisant l'architecture Medallion sur Databricks.

## 🏗️ Architecture
\`\`\`
Sources (JSON/CSV) → Auto Loader → Bronze → Silver → Gold → Dashboard
\`\`\`

## 🛠️ Technologies
- **Databricks** : Runtime 13.3 LTS
- **Delta Lake** : stockage ACID
- **Auto Loader** : ingestion incrémentale
- **DLT** : orchestration déclarative
- **Unity Catalog** : gouvernance
- **Python / SQL** : transformations

## 📊 Résultats
- Traitement de **2M+ lignes/jour**
- Latence de bout en bout : **< 5 minutes**
- **99.8%** de qualité des données (DLT expectations)

## 🚀 Comment exécuter
1. Cloner le repo
2. Importer les notebooks dans Databricks
3. Configurer les chemins source
4. Lancer le pipeline DLT

## 📸 Screenshots
[Architecture diagram, DLT pipeline view, quality dashboard]`} />
          </div>
        </section>

        {/* ══════════════════════════════════════════════════════════════
            SECTION 4 — CV
        ══════════════════════════════════════════════════════════════ */}
        <section className="mb-14">
          <h2 className="text-2xl font-bold text-[#1b3a4b] mb-4">
            📄 Optimiser son CV
          </h2>
          <p className="text-gray-700 leading-relaxed mb-6">
            Les recruteurs et ATS (systèmes de tri automatique) filtrent sur
            des mots-clés. Assurez-vous que les bons termes apparaissent dans
            votre CV.
          </p>

          <div className="bg-white rounded-xl border border-gray-200 p-6 mb-6">
            <h3 className="text-lg font-bold text-[#1b3a4b] mb-3">
              🔑 Mots-clés essentiels
            </h3>
            <div className="flex flex-wrap gap-2">
              {[
                "Delta Lake",
                "Spark SQL",
                "Databricks",
                "Unity Catalog",
                "DLT",
                "Auto Loader",
                "Structured Streaming",
                "Python",
                "SQL",
                "ETL/ELT",
                "Data Pipeline",
                "Lakehouse",
                "Medallion Architecture",
                "PySpark",
                "RGPD",
                "CI/CD",
                "Azure / AWS / GCP",
              ].map((kw) => (
                <span
                  key={kw}
                  className="inline-flex items-center px-3 py-1 rounded-full text-xs font-semibold bg-blue-50 text-blue-700 border border-blue-200"
                >
                  {kw}
                </span>
              ))}
            </div>
          </div>

          <div className="mb-6">
            <h3 className="text-lg font-bold text-[#1b3a4b] mb-3">
              📝 Exemple de rubrique compétences
            </h3>
            <CodeBlock language="text" code={`DATA ENGINEER - Databricks

Compétences techniques :
• Databricks (certifié DE Associate), Delta Lake, Spark SQL
• Pipelines ELT : Auto Loader, Structured Streaming, DLT
• Architecture Medallion (Bronze/Silver/Gold)
• Gouvernance : Unity Catalog, RGPD
• Orchestration : Databricks Workflows, Airflow
• Cloud : Azure / AWS / GCP
• Langages : Python, SQL, PySpark

Expérience projet :
• Conception et mise en production d'un pipeline Medallion
  traitant 2M+ événements/jour avec DLT et Auto Loader
• Migration d'un Data Warehouse legacy vers architecture
  Lakehouse Databricks (gain de 40% sur les coûts infra)
• Mise en place de Unity Catalog avec row-level security
  pour conformité RGPD dans le secteur bancaire`} />
          </div>

          <InfoBox type="info" title="À savoir">
            La certification Databricks Data Engineer Associate est un vrai
            plus sur le CV — mais l&apos;expérience projet compte encore plus.
            Mettez en avant des <strong>résultats chiffrés</strong> (volume
            traité, gain de performance, SLA).
          </InfoBox>
        </section>

        {/* ══════════════════════════════════════════════════════════════
            SECTION 5 — Jour J
        ══════════════════════════════════════════════════════════════ */}
        <section className="mb-14">
          <h2 className="text-2xl font-bold text-[#1b3a4b] mb-4">
            🎤 Conseils pour le jour J
          </h2>

          {/* Avant */}
          <div className="bg-white rounded-xl border border-gray-200 p-6 mb-6">
            <h3 className="text-lg font-bold text-[#1b3a4b] mb-3">
              📅 Avant l&apos;entretien
            </h3>
            <ul className="list-disc list-inside text-sm text-gray-700 space-y-2">
              <li>
                <strong>Recherchez l&apos;entreprise</strong> : consultez
                leur blog tech, leurs offres d&apos;emploi data, leur stack
                technique.
              </li>
              <li>
                <strong>Préparez 3-5 questions</strong> à poser au recruteur
                (signe de motivation).
              </li>
              <li>
                <strong>Révisez l&apos;architecture</strong> : soyez capable
                de dessiner un pipeline Medallion complet en 2 minutes.
              </li>
              <li>
                <strong>Entraînez-vous à voix haute</strong> : chronométrez
                vos réponses (2-3 min max par question).
              </li>
              <li>
                <strong>Préparez vos projets</strong> : ayez 2-3 exemples
                concrets prêts avec des chiffres.
              </li>
            </ul>
          </div>

          {/* Pendant */}
          <div className="bg-white rounded-xl border border-gray-200 p-6 mb-6">
            <h3 className="text-lg font-bold text-[#1b3a4b] mb-3">
              🎙️ Pendant l&apos;entretien
            </h3>
            <ul className="list-disc list-inside text-sm text-gray-700 space-y-2">
              <li>
                <strong>Méthode STAR</strong> : Situation → Task → Action →
                Result. Structurez chaque réponse ainsi.
              </li>
              <li>
                <strong>Dessinez des schémas</strong> : si c&apos;est en
                présentiel, utilisez le tableau blanc ; en visio, partagez
                un diagramme.
              </li>
              <li>
                <strong>Montrez votre enthousiasme</strong> : « J&apos;adore
                travailler avec Delta Lake parce que... »
              </li>
              <li>
                <strong>Expliquez le POURQUOI</strong> : ne dites pas juste
                « j&apos;utilise Z-ORDER », mais « j&apos;utilise Z-ORDER
                parce que nos requêtes filtrent souvent sur la colonne
                country, ce qui accélère le data skipping ».
              </li>
              <li>
                <strong>Admettez quand vous ne savez pas</strong> : « Je ne
                suis pas sûr, mais voici comment je procéderais pour trouver
                la réponse... »
              </li>
            </ul>
          </div>

          {/* Pièges */}
          <div className="bg-red-50 rounded-xl border border-red-200 p-6 mb-6">
            <h3 className="text-lg font-bold text-red-700 mb-3">
              ⚠️ Pièges à éviter
            </h3>
            <ul className="list-disc list-inside text-sm text-gray-700 space-y-2">
              <li>
                <strong>Lister les technologies sans contexte</strong> :
                « Je connais Spark, Delta Lake, DLT... » → Préférez :
                « J&apos;ai utilisé DLT pour fiabiliser un pipeline de 2M
                lignes/jour avec des expectations qui ont réduit les
                anomalies de 95% ».
              </li>
              <li>
                <strong>Ne pas poser de questions</strong> : cela suggère un
                manque d&apos;intérêt pour le poste.
              </li>
              <li>
                <strong>Sous-estimer les questions comportementales</strong> :
                { } les questions « soft skills » comptent autant que les
                questions techniques.
              </li>
              <li>
                <strong>Mentir sur ses compétences</strong> : un bon
                recruteur s&apos;en rendra compte. Soyez honnête sur votre
                niveau.
              </li>
              <li>
                <strong>Critiquer son ancien employeur</strong> : restez
                toujours positif et constructif.
              </li>
            </ul>
          </div>

          {/* Questions à poser */}
          <div className="bg-white rounded-xl border border-gray-200 p-6 mb-6">
            <h3 className="text-lg font-bold text-[#1b3a4b] mb-3">
              ❓ Questions à poser au recruteur
            </h3>
            <p className="text-sm text-gray-600 mb-3">
              Préparez au moins 3 questions. Cela montre votre curiosité et
              votre sérieux.
            </p>
            <ol className="list-decimal list-inside text-sm text-gray-700 space-y-2">
              <li>
                « Quelle est l&apos;architecture data actuelle et vers quoi
                vous souhaitez migrer ? »
              </li>
              <li>
                « Quelle est la taille de l&apos;équipe data et comment
                est-elle organisée ? »
              </li>
              <li>
                « Quels sont les principaux défis data que vous rencontrez
                aujourd&apos;hui ? »
              </li>
              <li>
                « Utilisez-vous Unity Catalog / DLT en production, ou
                c&apos;est prévu ? »
              </li>
              <li>
                « Comment se passe l&apos;onboarding pour un nouveau Data
                Engineer ? »
              </li>
            </ol>
          </div>
        </section>

        {/* ══════════════════════════════════════════════════════════════
            RÉCAPITULATIF
        ══════════════════════════════════════════════════════════════ */}
        <section className="mb-14">
          <div className="bg-gradient-to-br from-[#1b3a4b] to-[#2d5f7a] text-white rounded-2xl p-8">
            <h2 className="text-2xl font-bold mb-4">
              🎯 Récapitulatif : votre plan d&apos;action
            </h2>
            <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
              <div className="bg-white/10 rounded-lg p-4">
                <div className="text-2xl mb-2">📚</div>
                <h3 className="font-bold mb-1">Semaine 1-2</h3>
                <p className="text-sm text-white/80">
                  Maîtrisez les 20 questions techniques. Entraînez-vous à
                  voix haute 30 min/jour.
                </p>
              </div>
              <div className="bg-white/10 rounded-lg p-4">
                <div className="text-2xl mb-2">💻</div>
                <h3 className="font-bold mb-1">Semaine 3</h3>
                <p className="text-sm text-white/80">
                  Complétez le projet final SNCF. Publiez-le sur GitHub avec
                  un README soigné.
                </p>
              </div>
              <div className="bg-white/10 rounded-lg p-4">
                <div className="text-2xl mb-2">🎤</div>
                <h3 className="font-bold mb-1">Semaine 4</h3>
                <p className="text-sm text-white/80">
                  Entretiens blancs avec un ami. Optimisez votre CV et
                  LinkedIn. Postulez !
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
            href="/exercices/projet-final"
            className="inline-flex items-center gap-2 px-5 py-2.5 rounded-lg bg-[#ff3621] text-white font-semibold hover:bg-[#e02e1a] transition-colors"
          >
            ← Projet Final SNCF
          </Link>
        </div>
      </div>
    </div>
  );
}
