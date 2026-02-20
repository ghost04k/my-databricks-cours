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

export default function StreamingMultiHopExercicesPage() {
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
              📘 3 modules couverts
            </span>
          </div>
          <h1 className="text-3xl lg:text-4xl font-extrabold mb-3">
            🌊 Exercices : Streaming &amp; Architecture Multi-Hop
          </h1>
          <p className="text-lg text-white/80 max-w-2xl leading-relaxed">
            4 exercices progressifs pour maîtriser le streaming structuré,
            Auto Loader, l&apos;architecture Medallion (Bronze → Silver → Gold)
            et les fenêtres de temps.
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
                Premier Pipeline Streaming{" "}
                <span className="text-gray-400">(45 min)</span>
              </span>
            </li>
            <li className="flex items-center gap-2">
              <span className="w-6 h-6 flex items-center justify-center bg-[#1b3a4b] text-white text-xs font-bold rounded-full">
                2
              </span>
              <span>
                Auto Loader avec Évolution de Schéma{" "}
                <span className="text-gray-400">(45 min)</span>
              </span>
            </li>
            <li className="flex items-center gap-2">
              <span className="w-6 h-6 flex items-center justify-center bg-[#1b3a4b] text-white text-xs font-bold rounded-full">
                3
              </span>
              <span>
                Pipeline Medallion Complet{" "}
                <span className="text-gray-400">(1h30)</span>
              </span>
            </li>
            <li className="flex items-center gap-2">
              <span className="w-6 h-6 flex items-center justify-center bg-[#1b3a4b] text-white text-xs font-bold rounded-full">
                4
              </span>
              <span>
                Défi — Streaming avec Fenêtres{" "}
                <span className="text-gray-400">(1h)</span>
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
              Premier Pipeline Streaming
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
              Vous avez un répertoire contenant des fichiers JSON de commandes
              qui arrivent en continu. Votre objectif est de créer votre
              premier pipeline de streaming structuré pour filtrer et
              persister ces données en temps réel.
            </p>

            <h3 className="text-lg font-semibold text-[#1b3a4b]">
              📝 Instructions
            </h3>
            <ol className="list-decimal list-inside space-y-2 text-gray-700">
              <li>
                Créer une table source <code className="text-sm bg-gray-100 px-1.5 py-0.5 rounded">orders_raw</code> avec des données de test
              </li>
              <li>
                Lire la table en streaming avec <code className="text-sm bg-gray-100 px-1.5 py-0.5 rounded">spark.readStream</code>
              </li>
              <li>
                Appliquer un filtre pour ne garder que les commandes &gt; 50€
              </li>
              <li>
                Écrire le résultat en streaming avec un checkpoint
              </li>
            </ol>

            <InfoBox type="info" title="Streaming structuré">
              <p>
                Le streaming structuré de Spark traite les données en continu
                comme un DataFrame illimité. Avec{" "}
                <code className="text-sm bg-blue-100 px-1 py-0.5 rounded">trigger(availableNow=True)</code>,
                le stream traite toutes les données disponibles puis s&apos;arrête
                — idéal pour les exercices et les tests.
              </p>
            </InfoBox>

            <SolutionToggle id="sol-1">
              <p className="text-sm text-gray-700 mb-2">
                <strong>Solution complète :</strong>
              </p>

              <CodeBlock
                language="python"
                title="1. Créer des données source"
                code={`# 1. Créer des données source
spark.sql("""
CREATE OR REPLACE TABLE orders_raw (
  order_id INT,
  customer_id INT,
  amount DOUBLE,
  status STRING,
  order_date TIMESTAMP
)
""")

# Insérer des données de test
spark.sql("""
INSERT INTO orders_raw VALUES
  (1, 101, 75.50, 'completed', current_timestamp()),
  (2, 102, 30.00, 'pending', current_timestamp()),
  (3, 103, 150.00, 'completed', current_timestamp()),
  (4, 101, 45.00, 'completed', current_timestamp()),
  (5, 104, 200.00, 'shipped', current_timestamp())
""")`}
              />

              <CodeBlock
                language="python"
                title="2. Lire, filtrer et écrire en streaming"
                code={`# 2. Lire en streaming
stream_df = spark.readStream.table("orders_raw")

# 3. Filtrer les commandes > 50€
filtered_df = stream_df.filter("amount > 50")

# 4. Écrire en streaming
filtered_df.writeStream \\
    .trigger(availableNow=True) \\
    .outputMode("append") \\
    .option("checkpointLocation", "/tmp/checkpoint/orders_filtered") \\
    .table("orders_filtered") \\
    .awaitTermination()

# Vérifier le résultat
display(spark.sql("SELECT * FROM orders_filtered"))`}
              />

              <h4 className="text-sm font-semibold text-[#1b3a4b]">
                🔍 Explications :
              </h4>
              <ul className="list-disc list-inside text-sm text-gray-700 space-y-1">
                <li>
                  <code className="text-sm bg-gray-100 px-1 py-0.5 rounded">spark.readStream.table()</code> lit une table Delta en mode streaming incrémental.
                </li>
                <li>
                  <code className="text-sm bg-gray-100 px-1 py-0.5 rounded">filter()</code> s&apos;applique exactement comme sur un DataFrame statique.
                </li>
                <li>
                  Le <strong>checkpointLocation</strong> stocke la progression du stream pour garantir un traitement exactly-once.
                </li>
                <li>
                  <code className="text-sm bg-gray-100 px-1 py-0.5 rounded">awaitTermination()</code> bloque jusqu&apos;à la fin du micro-batch.
                </li>
              </ul>

              <h4 className="text-sm font-semibold text-[#1b3a4b] mt-3">
                ✅ Résultat attendu :
              </h4>
              <p className="text-sm text-gray-700">
                La table <code className="text-sm bg-gray-100 px-1 py-0.5 rounded">orders_filtered</code> contient
                3 lignes (order_id 1, 3 et 5) — seules les commandes &gt; 50€.
              </p>
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
              Auto Loader avec Évolution de Schéma
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
              Vous devez simuler l&apos;ingestion de fichiers JSON avec
              Auto Loader. Les fichiers de capteurs arrivent en lots successifs
              et le schéma peut évoluer au fil du temps (ajout de nouvelles
              colonnes).
            </p>

            <h3 className="text-lg font-semibold text-[#1b3a4b]">
              📝 Instructions
            </h3>
            <ol className="list-decimal list-inside space-y-2 text-gray-700">
              <li>
                Configurer Auto Loader pour lire des fichiers JSON
              </li>
              <li>
                Gérer l&apos;inférence de schéma automatique
              </li>
              <li>
                Ajouter un second lot avec une nouvelle colonne (<code className="text-sm bg-gray-100 px-1.5 py-0.5 rounded">humidity</code>) et observer l&apos;évolution du schéma
              </li>
            </ol>

            <InfoBox type="important" title="Auto Loader vs readStream">
              <p>
                Auto Loader (<code className="text-sm bg-red-100 px-1 py-0.5 rounded">cloudFiles</code>) est
                optimisé pour l&apos;ingestion de fichiers dans un Data Lake. Il
                découvre automatiquement les nouveaux fichiers, gère le schéma
                et suit la progression — bien plus efficace qu&apos;un{" "}
                <code className="text-sm bg-red-100 px-1 py-0.5 rounded">readStream.format(&quot;json&quot;)</code> classique.
              </p>
            </InfoBox>

            <SolutionToggle id="sol-2">
              <p className="text-sm text-gray-700 mb-2">
                <strong>Solution complète :</strong>
              </p>

              <CodeBlock
                language="python"
                title="Lot 1 : schéma initial"
                code={`import json

# Lot 1 : schéma initial
data_v1 = [
    {"sensor_id": 1, "temperature": 22.5, "timestamp": "2024-01-01T10:00:00"},
    {"sensor_id": 2, "temperature": 25.1, "timestamp": "2024-01-01T10:01:00"}
]

dbutils.fs.put("/tmp/sensors/batch1.json", 
    "\\n".join([json.dumps(d) for d in data_v1]), True)

# Auto Loader ingestion
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/tmp/schema/sensors")
    .load("/tmp/sensors/")
)

(df.writeStream
    .option("checkpointLocation", "/tmp/checkpoint/sensors")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .table("sensors_bronze")
    .awaitTermination()
)`}
              />

              <CodeBlock
                language="python"
                title="Lot 2 : nouveau champ humidity"
                code={`# Lot 2 : nouveau champ "humidity"
data_v2 = [
    {"sensor_id": 1, "temperature": 23.0, "humidity": 65.0, "timestamp": "2024-01-01T11:00:00"},
    {"sensor_id": 3, "temperature": 19.8, "humidity": 70.2, "timestamp": "2024-01-01T11:01:00"}
]

dbutils.fs.put("/tmp/sensors/batch2.json",
    "\\n".join([json.dumps(d) for d in data_v2]), True)

# Re-run Auto Loader - le schéma évolue automatiquement !
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/tmp/schema/sensors")
    .load("/tmp/sensors/")
)

(df.writeStream
    .option("checkpointLocation", "/tmp/checkpoint/sensors")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .table("sensors_bronze")
    .awaitTermination()
)

# Vérifier l'évolution du schéma
display(spark.sql("SELECT * FROM sensors_bronze"))
spark.sql("DESCRIBE sensors_bronze").show()`}
              />

              <InfoBox type="tip" title="Option mergeSchema">
                <p>
                  L&apos;option <code className="text-sm bg-emerald-100 px-1 py-0.5 rounded">mergeSchema</code> permet
                  à Delta Lake d&apos;accepter automatiquement les nouvelles colonnes
                  lors de l&apos;écriture. Sans cette option, l&apos;ajout d&apos;une colonne
                  <code className="text-sm bg-emerald-100 px-1 py-0.5 rounded"> humidity</code> provoquerait une erreur.
                </p>
              </InfoBox>

              <h4 className="text-sm font-semibold text-[#1b3a4b]">
                🔍 Explications :
              </h4>
              <ul className="list-disc list-inside text-sm text-gray-700 space-y-1">
                <li>
                  <code className="text-sm bg-gray-100 px-1 py-0.5 rounded">cloudFiles.schemaLocation</code> stocke
                  le schéma inféré pour le réutiliser entre les exécutions.
                </li>
                <li>
                  Auto Loader détecte automatiquement les nouveaux fichiers
                  dans le répertoire sans re-traiter les anciens.
                </li>
                <li>
                  Après le lot 2, la colonne <code className="text-sm bg-gray-100 px-1 py-0.5 rounded">humidity</code> apparaît
                  dans le schéma. Les lignes du lot 1 auront <code className="text-sm bg-gray-100 px-1 py-0.5 rounded">null</code> pour cette colonne.
                </li>
              </ul>
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
              Pipeline Medallion Complet
            </h2>
          </div>
          <div className="flex items-center gap-3 mb-5 ml-11">
            <span className="text-xs font-medium bg-gray-100 text-gray-600 px-2.5 py-1 rounded-full">
              ⏱ 1h30
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
              Construisez un pipeline complet suivant l&apos;architecture
              Medallion (<strong>Bronze → Silver → Gold</strong>) pour des
              données de ventes. Chaque couche a un rôle précis dans le
              traitement des données.
            </p>

            <h3 className="text-lg font-semibold text-[#1b3a4b]">
              📝 Instructions
            </h3>
            <ol className="list-decimal list-inside space-y-2 text-gray-700">
              <li>
                <strong>Bronze</strong> : Ingestion des données brutes
                (append-only, aucune transformation)
              </li>
              <li>
                <strong>Silver</strong> : Nettoyage — supprimer les nulls,
                dédupliquer par <code className="text-sm bg-gray-100 px-1.5 py-0.5 rounded">order_id</code>,
                caster les types correctement
              </li>
              <li>
                <strong>Gold</strong> : Agrégations métier — chiffre d&apos;affaires
                par catégorie, nombre de commandes par jour
              </li>
            </ol>

            <InfoBox type="warning" title="Architecture Medallion">
              <p>
                Chaque couche a son propre <strong>checkpoint</strong>. Ne
                partagez jamais un même répertoire de checkpoint entre
                plusieurs streams — cela causerait des conflits et des pertes
                de données.
              </p>
            </InfoBox>

            <SolutionToggle id="sol-3">
              <p className="text-sm text-gray-700 mb-2">
                <strong>Étape 1 — Bronze : Ingestion brute</strong>
              </p>

              <CodeBlock
                language="python"
                title="Bronze : Ingestion brute"
                code={`# Bronze : Ingestion brute
bronze_df = spark.readStream.table("sales_raw")

bronze_df.writeStream \\
    .trigger(availableNow=True) \\
    .outputMode("append") \\
    .option("checkpointLocation", "/tmp/checkpoint/sales_bronze") \\
    .table("sales_bronze") \\
    .awaitTermination()`}
              />

              <p className="text-sm text-gray-700 mt-4 mb-2">
                <strong>Étape 2 — Silver : Nettoyage et enrichissement</strong>
              </p>

              <CodeBlock
                language="python"
                title="Silver : Nettoyage et enrichissement"
                code={`# Silver : Nettoyage et enrichissement
from pyspark.sql.functions import col, current_timestamp

silver_df = (spark.readStream
    .table("sales_bronze")
    .filter("order_id IS NOT NULL AND amount > 0")
    .dropDuplicates(["order_id"])
    .withColumn("ingestion_time", current_timestamp())
    .select(
        col("order_id").cast("int"),
        col("product").cast("string"),
        col("category").cast("string"),
        col("amount").cast("double"),
        col("order_date").cast("date"),
        "ingestion_time"
    )
)

silver_df.writeStream \\
    .trigger(availableNow=True) \\
    .outputMode("append") \\
    .option("checkpointLocation", "/tmp/checkpoint/sales_silver") \\
    .table("sales_silver") \\
    .awaitTermination()`}
              />

              <p className="text-sm text-gray-700 mt-4 mb-2">
                <strong>Étape 3 — Gold : Agrégations métier</strong>
              </p>

              <CodeBlock
                language="python"
                title="Gold : Agrégations métier"
                code={`# Gold : Agrégations métier
from pyspark.sql.functions import sum, count, avg

# CA par catégorie
gold_category = (spark.readStream
    .table("sales_silver")
    .groupBy("category")
    .agg(
        sum("amount").alias("total_revenue"),
        count("*").alias("total_orders"),
        avg("amount").alias("avg_order_value")
    )
)

gold_category.writeStream \\
    .trigger(availableNow=True) \\
    .outputMode("complete") \\
    .option("checkpointLocation", "/tmp/checkpoint/sales_gold_category") \\
    .table("gold_revenue_by_category") \\
    .awaitTermination()`}
              />

              <h4 className="text-sm font-semibold text-[#1b3a4b] mt-3">
                🔍 Explications :
              </h4>
              <ul className="list-disc list-inside text-sm text-gray-700 space-y-1">
                <li>
                  <strong>Bronze</strong> : Copie brute, aucune transformation.
                  Sert de source de vérité immuable.
                </li>
                <li>
                  <strong>Silver</strong> : Les données sont nettoyées, typées
                  et dédupliquées. Le{" "}
                  <code className="text-sm bg-gray-100 px-1 py-0.5 rounded">dropDuplicates</code> garantit
                  l&apos;unicité sur <code className="text-sm bg-gray-100 px-1 py-0.5 rounded">order_id</code>.
                </li>
                <li>
                  <strong>Gold</strong> : Les agrégations utilisent{" "}
                  <code className="text-sm bg-gray-100 px-1 py-0.5 rounded">outputMode(&quot;complete&quot;)</code> car
                  les résultats des <code className="text-sm bg-gray-100 px-1 py-0.5 rounded">groupBy</code> doivent
                  être réécrits intégralement à chaque micro-batch.
                </li>
              </ul>

              <InfoBox type="tip" title="Bonnes pratiques">
                <p>
                  En production, utilisez des noms de checkpoint explicites et
                  persistants (ex:{" "}
                  <code className="text-sm bg-emerald-100 px-1 py-0.5 rounded">dbfs:/checkpoints/prod/sales_silver</code>).
                  Évitez <code className="text-sm bg-emerald-100 px-1 py-0.5 rounded">/tmp</code> qui est éphémère.
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
              Défi — Streaming avec Fenêtres
            </h2>
          </div>
          <div className="flex items-center gap-3 mb-5 ml-11">
            <span className="text-xs font-medium bg-gray-100 text-gray-600 px-2.5 py-1 rounded-full">
              ⏱ 1h
            </span>
            <span className="text-xs font-medium bg-orange-100 text-orange-700 px-2.5 py-1 rounded-full">
              Avancé
            </span>
          </div>

          <div className="ml-11 space-y-4">
            <h3 className="text-lg font-semibold text-[#1b3a4b]">
              📖 Contexte
            </h3>
            <p className="text-gray-700 leading-relaxed">
              Vous recevez des données de clics web en streaming. Votre
              objectif est de calculer le nombre de clics par page dans des
              fenêtres de 5 minutes, avec un watermark de 10 minutes pour
              gérer les données en retard.
            </p>

            <h3 className="text-lg font-semibold text-[#1b3a4b]">
              📝 Instructions
            </h3>
            <ol className="list-decimal list-inside space-y-2 text-gray-700">
              <li>
                Lire la table <code className="text-sm bg-gray-100 px-1.5 py-0.5 rounded">clicks_raw</code> en streaming
              </li>
              <li>
                Appliquer un watermark de 10 minutes sur la colonne{" "}
                <code className="text-sm bg-gray-100 px-1.5 py-0.5 rounded">click_time</code>
              </li>
              <li>
                Grouper par fenêtre de 5 minutes et par{" "}
                <code className="text-sm bg-gray-100 px-1.5 py-0.5 rounded">page_url</code>
              </li>
              <li>
                Compter les clics par groupe
              </li>
              <li>
                Écrire les résultats dans une table
              </li>
            </ol>

            <InfoBox type="important" title="Watermark et fenêtres">
              <p>
                Le <strong>watermark</strong> définit le délai maximal accepté
                pour les données en retard. Au-delà de ce délai, les
                événements tardifs sont ignorés. Les <strong>fenêtres</strong>{" "}
                (windows) découpent le temps en intervalles fixes pour les
                agrégations.
              </p>
            </InfoBox>

            <SolutionToggle id="sol-4">
              <p className="text-sm text-gray-700 mb-2">
                <strong>Solution complète :</strong>
              </p>

              <CodeBlock
                language="python"
                title="Streaming avec fenêtres de temps"
                code={`from pyspark.sql.functions import window, count

clicks_stream = spark.readStream.table("clicks_raw")

windowed = (clicks_stream
    .withWatermark("click_time", "10 minutes")
    .groupBy(
        window("click_time", "5 minutes"),
        "page_url"
    )
    .agg(count("*").alias("click_count"))
)

windowed.writeStream \\
    .trigger(availableNow=True) \\
    .outputMode("append") \\
    .option("checkpointLocation", "/tmp/checkpoint/clicks_windowed") \\
    .table("clicks_per_page_5min") \\
    .awaitTermination()

# Vérifier les résultats
display(spark.sql("SELECT * FROM clicks_per_page_5min ORDER BY window"))`}
              />

              <h4 className="text-sm font-semibold text-[#1b3a4b] mt-3">
                🔍 Explications :
              </h4>
              <ul className="list-disc list-inside text-sm text-gray-700 space-y-1">
                <li>
                  <code className="text-sm bg-gray-100 px-1 py-0.5 rounded">withWatermark(&quot;click_time&quot;, &quot;10 minutes&quot;)</code>{" "}
                  indique que les événements arrivant avec plus de 10 minutes
                  de retard sont ignorés.
                </li>
                <li>
                  <code className="text-sm bg-gray-100 px-1 py-0.5 rounded">window(&quot;click_time&quot;, &quot;5 minutes&quot;)</code>{" "}
                  crée des fenêtres temporelles de 5 minutes (ex: 10:00–10:05,
                  10:05–10:10, etc.).
                </li>
                <li>
                  L&apos;<code className="text-sm bg-gray-100 px-1 py-0.5 rounded">outputMode(&quot;append&quot;)</code> est
                  utilisé avec le watermark : une fenêtre n&apos;est émise qu&apos;une
                  fois que le watermark l&apos;a déclarée complète.
                </li>
                <li>
                  Le résultat contient une colonne <code className="text-sm bg-gray-100 px-1 py-0.5 rounded">window</code> structurée
                  avec <code className="text-sm bg-gray-100 px-1 py-0.5 rounded">start</code> et{" "}
                  <code className="text-sm bg-gray-100 px-1 py-0.5 rounded">end</code>.
                </li>
              </ul>

              <InfoBox type="tip" title="Fenêtres glissantes">
                <p>
                  Vous pouvez aussi utiliser des fenêtres glissantes (sliding
                  windows) en ajoutant un troisième paramètre :{" "}
                  <code className="text-sm bg-emerald-100 px-1 py-0.5 rounded">
                    window(&quot;click_time&quot;, &quot;10 minutes&quot;, &quot;5 minutes&quot;)
                  </code>{" "}
                  crée des fenêtres de 10 min qui avancent de 5 min à chaque
                  pas.
                </p>
              </InfoBox>
            </SolutionToggle>
          </div>
        </section>

        {/* Navigation bottom */}
        <div className="mt-16 pt-8 border-t border-gray-200">
          <div className="flex flex-col sm:flex-row justify-between gap-4">
            <Link
              href="/exercices"
              className="inline-flex items-center gap-2 px-5 py-3 rounded-lg bg-gray-100 text-[#1b3a4b] font-semibold hover:bg-gray-200 transition-colors text-sm"
            >
              ← Tous les exercices
            </Link>
            <Link
              href="/programme"
              className="inline-flex items-center gap-2 px-5 py-3 rounded-lg bg-[#1b3a4b] text-white font-semibold hover:bg-[#2d5f7a] transition-colors text-sm"
            >
              📅 Voir le programme complet →
            </Link>
          </div>
        </div>
      </div>
    </div>
  );
}
