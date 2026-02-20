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

export default function ProjetEcommercePage() {
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
            <span className="text-sm text-white/70">⏱ 6 heures</span>
            <span className="text-sm text-white/70">📅 Jour 9</span>
          </div>
          <h1 className="text-3xl lg:text-4xl font-extrabold mb-3">
            🛒 Mini-Projet : Pipeline E-commerce
          </h1>
          <p className="text-lg text-white/80 max-w-2xl leading-relaxed">
            Créez un pipeline de données complet pour analyser les performances
            d&apos;un e-commerce français — de l&apos;ingestion multi-format à
            la couche Gold avec KPIs business.
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

        {/* Contexte du projet */}
        <section className="mb-10">
          <h2 className="text-2xl font-bold text-[#1b3a4b] mb-4">
            📖 Contexte du projet
          </h2>
          <p className="text-gray-700 leading-relaxed mb-4">
            Vous êtes <strong>Data Engineer</strong> chez un e-commerce
            français. Vous devez créer un pipeline de données complet pour
            analyser les performances de l&apos;entreprise. Les données
            arrivent sous forme de fichiers <strong>JSON</strong> (commandes),{" "}
            <strong>CSV</strong> (clients) et <strong>Parquet</strong>{" "}
            (produits).
          </p>

          <div className="bg-gray-50 rounded-xl border border-gray-200 p-5 mb-6">
            <h3 className="text-lg font-bold text-[#1b3a4b] mb-3">
              🎯 Objectifs
            </h3>
            <ul className="space-y-2 text-sm text-gray-700">
              <li className="flex items-center gap-2">
                <span className="text-green-500">✓</span>
                Ingérer 3 sources de données différentes (JSON, CSV, Parquet)
              </li>
              <li className="flex items-center gap-2">
                <span className="text-green-500">✓</span>
                Implémenter l&apos;architecture Medallion (Bronze → Silver →
                Gold)
              </li>
              <li className="flex items-center gap-2">
                <span className="text-green-500">✓</span>
                Créer des KPIs business exploitables
              </li>
              <li className="flex items-center gap-2">
                <span className="text-green-500">✓</span>
                Assurer la qualité des données à chaque couche
              </li>
            </ul>
          </div>
        </section>

        {/* Architecture */}
        <section className="mb-10">
          <h2 className="text-2xl font-bold text-[#1b3a4b] mb-4">
            🏗️ Architecture du projet
          </h2>
          <div className="bg-white rounded-xl border border-gray-200 p-6 overflow-x-auto">
            <div className="flex items-center justify-between gap-3 min-w-[600px]">
              {/* Sources */}
              <div className="flex flex-col items-center gap-2">
                <div className="bg-purple-100 border-2 border-purple-300 rounded-xl px-4 py-3 text-center min-w-[120px]">
                  <div className="text-2xl mb-1">📁</div>
                  <div className="font-bold text-purple-800 text-sm">
                    Sources
                  </div>
                  <div className="text-xs text-purple-600 mt-1">
                    JSON / CSV / Parquet
                  </div>
                </div>
              </div>

              <div className="text-2xl text-gray-400 font-bold">→</div>

              {/* Bronze */}
              <div className="flex flex-col items-center gap-2">
                <div className="bg-amber-100 border-2 border-amber-300 rounded-xl px-4 py-3 text-center min-w-[120px]">
                  <div className="text-2xl mb-1">🥉</div>
                  <div className="font-bold text-amber-800 text-sm">
                    Bronze
                  </div>
                  <div className="text-xs text-amber-600 mt-1">
                    Données brutes
                  </div>
                </div>
              </div>

              <div className="text-2xl text-gray-400 font-bold">→</div>

              {/* Silver */}
              <div className="flex flex-col items-center gap-2">
                <div className="bg-slate-100 border-2 border-slate-300 rounded-xl px-4 py-3 text-center min-w-[120px]">
                  <div className="text-2xl mb-1">🥈</div>
                  <div className="font-bold text-slate-700 text-sm">
                    Silver
                  </div>
                  <div className="text-xs text-slate-500 mt-1">
                    Nettoyé + Enrichi
                  </div>
                </div>
              </div>

              <div className="text-2xl text-gray-400 font-bold">→</div>

              {/* Gold */}
              <div className="flex flex-col items-center gap-2">
                <div className="bg-yellow-100 border-2 border-yellow-300 rounded-xl px-4 py-3 text-center min-w-[120px]">
                  <div className="text-2xl mb-1">🥇</div>
                  <div className="font-bold text-yellow-800 text-sm">Gold</div>
                  <div className="text-xs text-yellow-600 mt-1">
                    KPIs Analytics
                  </div>
                </div>
              </div>
            </div>
          </div>
        </section>

        {/* Sommaire */}
        <div className="bg-gray-50 rounded-xl border border-gray-200 p-5 mb-10">
          <h2 className="text-lg font-bold text-[#1b3a4b] mb-3">
            📋 Sommaire des étapes
          </h2>
          <ol className="space-y-2 text-sm text-gray-700">
            <li className="flex items-center gap-2">
              <span className="w-6 h-6 flex items-center justify-center bg-[#1b3a4b] text-white text-xs font-bold rounded-full">
                1
              </span>
              <span>
                Préparation des données{" "}
                <span className="text-gray-400">(30 min)</span>
              </span>
            </li>
            <li className="flex items-center gap-2">
              <span className="w-6 h-6 flex items-center justify-center bg-[#1b3a4b] text-white text-xs font-bold rounded-full">
                2
              </span>
              <span>
                Couche Bronze — Ingestion{" "}
                <span className="text-gray-400">(45 min)</span>
              </span>
            </li>
            <li className="flex items-center gap-2">
              <span className="w-6 h-6 flex items-center justify-center bg-[#1b3a4b] text-white text-xs font-bold rounded-full">
                3
              </span>
              <span>
                Couche Silver — Nettoyage &amp; Enrichissement{" "}
                <span className="text-gray-400">(1h)</span>
              </span>
            </li>
            <li className="flex items-center gap-2">
              <span className="w-6 h-6 flex items-center justify-center bg-[#1b3a4b] text-white text-xs font-bold rounded-full">
                4
              </span>
              <span>
                Couche Gold — KPIs Business{" "}
                <span className="text-gray-400">(1h)</span>
              </span>
            </li>
            <li className="flex items-center gap-2">
              <span className="w-6 h-6 flex items-center justify-center bg-[#1b3a4b] text-white text-xs font-bold rounded-full">
                5
              </span>
              <span>
                Validation &amp; Qualité{" "}
                <span className="text-gray-400">(30 min)</span>
              </span>
            </li>
          </ol>
        </div>

        {/* ====================== ÉTAPE 1 ====================== */}
        <section className="mb-14">
          <div className="flex items-center gap-3 mb-2">
            <span className="w-8 h-8 flex items-center justify-center bg-[#ff3621] text-white text-sm font-bold rounded-full">
              1
            </span>
            <h2 className="text-2xl font-bold text-[#1b3a4b]">
              Préparation des données
            </h2>
          </div>
          <div className="flex items-center gap-3 mb-5 ml-11">
            <span className="text-xs font-medium bg-gray-100 text-gray-600 px-2.5 py-1 rounded-full">
              ⏱ 30 min
            </span>
          </div>

          <div className="ml-11 space-y-4">
            <h3 className="text-lg font-semibold text-[#1b3a4b]">
              📖 Objectif
            </h3>
            <p className="text-gray-700 leading-relaxed">
              Créer les données source simulées pour les trois entités :
              clients (CSV), produits (Parquet) et commandes (JSON par lots).
              Ces données seront ingérées dans les étapes suivantes.
            </p>

            <InfoBox type="info" title="Formats multiples">
              Dans un cas réel, les données proviennent souvent de systèmes
              différents avec des formats variés. C&apos;est pourquoi nous
              simulons 3 formats : JSON, CSV et Parquet.
            </InfoBox>

            <h3 className="text-lg font-semibold text-[#1b3a4b]">
              📝 Instructions
            </h3>
            <ol className="list-decimal list-inside space-y-2 text-gray-700">
              <li>
                Créez un notebook Python sur Databricks et attachez-le à
                votre cluster.
              </li>
              <li>
                Créez la base de données <code>ecommerce</code> si elle
                n&apos;existe pas déjà.
              </li>
              <li>
                Générez les données clients au format CSV, les produits au
                format Parquet, et les commandes au format JSON (5 lots de 20
                commandes).
              </li>
            </ol>

            <SolutionToggle id="sol-etape1">
              <p className="text-sm font-semibold text-gray-600 mb-2">
                Code complet — Préparation des données :
              </p>
              <CodeBlock
                language="python"
                title="Création de la base et des données source"
                code={`# Créer la base de données
spark.sql("CREATE DATABASE IF NOT EXISTS ecommerce")
spark.sql("USE ecommerce")`}
              />
              <CodeBlock
                language="python"
                title="Données Clients (CSV)"
                code={`clients_data = """customer_id,name,email,city,segment,registration_date
C001,Marie Dupont,marie@email.com,Paris,Premium,2023-01-15
C002,Jean Martin,jean@email.com,Lyon,Standard,2023-03-20
C003,Sophie Bernard,sophie@email.com,Marseille,Premium,2023-02-10
C004,Pierre Durand,pierre@email.com,Toulouse,Standard,2023-04-05
C005,Claire Moreau,claire@email.com,Bordeaux,Premium,2023-01-30
C006,Luc Petit,luc@email.com,Nice,Standard,2023-05-12
C007,Emma Laurent,emma@email.com,Paris,Premium,2023-02-28
C008,Thomas Roux,thomas@email.com,Lyon,Standard,2023-06-01
C009,Julie Fournier,julie@email.com,Nantes,Premium,2023-03-15
C010,Marc Girard,marc@email.com,Strasbourg,Standard,2023-07-20"""

dbutils.fs.put("/tmp/ecommerce/clients/clients.csv", clients_data, True)`}
              />
              <CodeBlock
                language="python"
                title="Données Produits (Parquet)"
                code={`products = [
    {"product_id": "P001", "name": "Laptop Pro 15", "category": "Electronics", "price": 1299.99, "brand": "TechBrand"},
    {"product_id": "P002", "name": "Smartphone X", "category": "Electronics", "price": 799.99, "brand": "TechBrand"},
    {"product_id": "P003", "name": "Casque Audio", "category": "Electronics", "price": 149.99, "brand": "SoundMax"},
    {"product_id": "P004", "name": "T-shirt Coton", "category": "Clothing", "price": 29.99, "brand": "FashionCo"},
    {"product_id": "P005", "name": "Book: Python", "category": "Books", "price": 39.99, "brand": "EdTech"},
    {"product_id": "P006", "name": "Desk Lamp", "category": "Home", "price": 59.99, "brand": "HomeBright"},
]

products_df = spark.createDataFrame(products)
products_df.write.format("parquet").mode("overwrite").save("/tmp/ecommerce/products/")`}
              />
              <CodeBlock
                language="python"
                title="Données Commandes (JSON par lots)"
                code={`import json
from datetime import datetime, timedelta
import random

orders = []
for i in range(1, 101):
    order = {
        "order_id": f"ORD{i:04d}",
        "customer_id": f"C{random.randint(1,10):03d}",
        "product_id": f"P{random.randint(1,6):03d}",
        "quantity": random.randint(1, 5),
        "order_date": (datetime(2024, 1, 1) + timedelta(days=random.randint(0, 180))).isoformat(),
        "status": random.choice(["completed", "pending", "shipped", "cancelled"]),
        "payment_method": random.choice(["card", "paypal", "bank_transfer"])
    }
    orders.append(order)

# Écriture par lots de 20
for batch_num in range(5):
    batch = orders[batch_num*20:(batch_num+1)*20]
    dbutils.fs.put(
        f"/tmp/ecommerce/orders/batch_{batch_num}.json",
        "\\n".join([json.dumps(o) for o in batch]), True
    )

print(f"✅ {len(orders)} commandes générées en 5 lots")`}
              />
            </SolutionToggle>

            <InfoBox type="tip" title="Astuce">
              Utilisez <code>display(spark.read.json(&quot;/tmp/ecommerce/orders/&quot;))</code>{" "}
              pour vérifier rapidement que vos données ont été créées
              correctement.
            </InfoBox>
          </div>
        </section>

        {/* ====================== ÉTAPE 2 ====================== */}
        <section className="mb-14">
          <div className="flex items-center gap-3 mb-2">
            <span className="w-8 h-8 flex items-center justify-center bg-[#ff3621] text-white text-sm font-bold rounded-full">
              2
            </span>
            <h2 className="text-2xl font-bold text-[#1b3a4b]">
              Couche Bronze — Ingestion
            </h2>
          </div>
          <div className="flex items-center gap-3 mb-5 ml-11">
            <span className="text-xs font-medium bg-gray-100 text-gray-600 px-2.5 py-1 rounded-full">
              ⏱ 45 min
            </span>
          </div>

          <div className="ml-11 space-y-4">
            <h3 className="text-lg font-semibold text-[#1b3a4b]">
              📖 Objectif
            </h3>
            <p className="text-gray-700 leading-relaxed">
              Ingérer les données brutes dans la couche Bronze. Utilisez{" "}
              <strong>Auto Loader</strong> pour les commandes (streaming) et
              une lecture batch pour les clients et produits. Ajoutez des
              métadonnées d&apos;ingestion à chaque table.
            </p>

            <InfoBox type="info" title="Architecture Medallion — Bronze">
              La couche Bronze contient les données brutes telles quelles,
              avec des métadonnées d&apos;ingestion (fichier source,
              horodatage). Aucune transformation n&apos;est appliquée à ce
              stade.
            </InfoBox>

            <h3 className="text-lg font-semibold text-[#1b3a4b]">
              📝 Instructions
            </h3>
            <ol className="list-decimal list-inside space-y-2 text-gray-700">
              <li>
                Ingérez les <strong>commandes</strong> avec Auto Loader
                (format <code>cloudFiles</code>) en streaming.
              </li>
              <li>
                Ingérez les <strong>clients</strong> en batch depuis le
                fichier CSV.
              </li>
              <li>
                Ingérez les <strong>produits</strong> en batch depuis les
                fichiers Parquet.
              </li>
              <li>
                Ajoutez une colonne <code>_ingestion_time</code> à chaque
                table Bronze.
              </li>
            </ol>

            <SolutionToggle id="sol-etape2">
              <p className="text-sm font-semibold text-gray-600 mb-2">
                Code complet — Couche Bronze :
              </p>
              <CodeBlock
                language="python"
                title="Bronze — Commandes (Auto Loader / Streaming)"
                code={`from pyspark.sql.functions import current_timestamp, input_file_name

# Bronze - Commandes (Auto Loader)
orders_bronze = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/tmp/schema/orders")
    .load("/tmp/ecommerce/orders/")
    .withColumn("_source_file", input_file_name())
    .withColumn("_ingestion_time", current_timestamp())
)

orders_bronze.writeStream \\
    .option("checkpointLocation", "/tmp/checkpoint/orders_bronze") \\
    .trigger(availableNow=True) \\
    .table("ecommerce.bronze_orders") \\
    .awaitTermination()

print("✅ Bronze Orders ingérées")`}
              />
              <CodeBlock
                language="python"
                title="Bronze — Clients (Batch CSV)"
                code={`# Bronze - Clients (batch)
clients_bronze = (spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/tmp/ecommerce/clients/")
    .withColumn("_ingestion_time", current_timestamp())
)
clients_bronze.write.mode("overwrite").saveAsTable("ecommerce.bronze_clients")

print("✅ Bronze Clients ingérés")`}
              />
              <CodeBlock
                language="python"
                title="Bronze — Produits (Batch Parquet)"
                code={`# Bronze - Produits (batch)
products_bronze = (spark.read
    .format("parquet")
    .load("/tmp/ecommerce/products/")
    .withColumn("_ingestion_time", current_timestamp())
)
products_bronze.write.mode("overwrite").saveAsTable("ecommerce.bronze_products")

print("✅ Bronze Produits ingérés")`}
              />
            </SolutionToggle>

            <InfoBox type="warning" title="Checkpoint">
              N&apos;oubliez pas de spécifier un <code>checkpointLocation</code>{" "}
              unique pour chaque stream. Sans cela, le streaming ne pourra
              pas reprendre en cas d&apos;interruption.
            </InfoBox>
          </div>
        </section>

        {/* ====================== ÉTAPE 3 ====================== */}
        <section className="mb-14">
          <div className="flex items-center gap-3 mb-2">
            <span className="w-8 h-8 flex items-center justify-center bg-[#ff3621] text-white text-sm font-bold rounded-full">
              3
            </span>
            <h2 className="text-2xl font-bold text-[#1b3a4b]">
              Couche Silver — Nettoyage &amp; Enrichissement
            </h2>
          </div>
          <div className="flex items-center gap-3 mb-5 ml-11">
            <span className="text-xs font-medium bg-gray-100 text-gray-600 px-2.5 py-1 rounded-full">
              ⏱ 1h
            </span>
          </div>

          <div className="ml-11 space-y-4">
            <h3 className="text-lg font-semibold text-[#1b3a4b]">
              📖 Objectif
            </h3>
            <p className="text-gray-700 leading-relaxed">
              Nettoyer et enrichir les données. Filtrez les valeurs nulles,
              supprimez les doublons, normalisez les formats, puis enrichissez
              les commandes avec les données clients et produits via des{" "}
              <strong>jointures</strong>.
            </p>

            <InfoBox type="info" title="Architecture Medallion — Silver">
              La couche Silver applique des règles de validation et de
              nettoyage : suppression des nulls, déduplication, normalisation
              des formats. C&apos;est aussi ici qu&apos;on enrichit les
              données par jointure entre les tables.
            </InfoBox>

            <h3 className="text-lg font-semibold text-[#1b3a4b]">
              📝 Instructions
            </h3>
            <ol className="list-decimal list-inside space-y-2 text-gray-700">
              <li>
                Nettoyez les commandes : filtrez les nulls, supprimez les
                commandes annulées, dédupliquez sur{" "}
                <code>order_id</code>.
              </li>
              <li>
                Nettoyez les clients : normalisez les noms (trim) et les
                villes (majuscules).
              </li>
              <li>
                Créez une table enrichie{" "}
                <code>silver_orders_enriched</code> en joignant commandes,
                clients et produits.
              </li>
              <li>
                Calculez le montant total par commande :{" "}
                <code>price × quantity</code>.
              </li>
            </ol>

            <SolutionToggle id="sol-etape3">
              <p className="text-sm font-semibold text-gray-600 mb-2">
                Code complet — Couche Silver :
              </p>
              <CodeBlock
                language="python"
                title="Silver — Commandes (nettoyage streaming)"
                code={`from pyspark.sql.functions import col, current_timestamp, to_date, trim, upper

# Silver - Commandes
orders_silver = (spark.readStream
    .table("ecommerce.bronze_orders")
    .filter("order_id IS NOT NULL AND customer_id IS NOT NULL")
    .filter("status != 'cancelled'")
    .dropDuplicates(["order_id"])
    .withColumn("order_date", to_date(col("order_date")))
    .withColumn("processed_at", current_timestamp())
)

orders_silver.writeStream \\
    .option("checkpointLocation", "/tmp/checkpoint/orders_silver") \\
    .trigger(availableNow=True) \\
    .table("ecommerce.silver_orders") \\
    .awaitTermination()

print("✅ Silver Orders nettoyées")`}
              />
              <CodeBlock
                language="python"
                title="Silver — Clients (nettoyage batch)"
                code={`# Silver - Clients (nettoyage)
clients_silver = (spark.table("ecommerce.bronze_clients")
    .filter("customer_id IS NOT NULL")
    .withColumn("name", trim(col("name")))
    .withColumn("city", upper(trim(col("city"))))
    .dropDuplicates(["customer_id"])
)
clients_silver.write.mode("overwrite").saveAsTable("ecommerce.silver_clients")

print("✅ Silver Clients nettoyés")`}
              />
              <CodeBlock
                language="python"
                title="Silver — Enrichissement (jointures)"
                code={`# Silver - Enrichissement des commandes avec clients et produits
enriched_orders = (spark.table("ecommerce.silver_orders")
    .join(spark.table("ecommerce.silver_clients"), "customer_id", "left")
    .join(spark.table("ecommerce.bronze_products"), "product_id", "left")
    .select(
        "order_id", "customer_id", "name", "city", "segment",
        "product_id", col("bronze_products.name").alias("product_name"),
        "category", "price", "quantity",
        (col("price") * col("quantity")).alias("total_amount"),
        "order_date", "status", "payment_method"
    )
)
enriched_orders.write.mode("overwrite").saveAsTable("ecommerce.silver_orders_enriched")

print("✅ Silver Orders Enriched créée")`}
              />
            </SolutionToggle>

            <InfoBox type="tip" title="Conseil">
              Utilisez des jointures <code>LEFT</code> plutôt que{" "}
              <code>INNER</code> pour ne pas perdre de commandes si un
              client ou produit n&apos;est pas trouvé. Vous pourrez détecter
              ces cas dans la couche Gold.
            </InfoBox>
          </div>
        </section>

        {/* ====================== ÉTAPE 4 ====================== */}
        <section className="mb-14">
          <div className="flex items-center gap-3 mb-2">
            <span className="w-8 h-8 flex items-center justify-center bg-[#ff3621] text-white text-sm font-bold rounded-full">
              4
            </span>
            <h2 className="text-2xl font-bold text-[#1b3a4b]">
              Couche Gold — KPIs Business
            </h2>
          </div>
          <div className="flex items-center gap-3 mb-5 ml-11">
            <span className="text-xs font-medium bg-gray-100 text-gray-600 px-2.5 py-1 rounded-full">
              ⏱ 1h
            </span>
          </div>

          <div className="ml-11 space-y-4">
            <h3 className="text-lg font-semibold text-[#1b3a4b]">
              📖 Objectif
            </h3>
            <p className="text-gray-700 leading-relaxed">
              Créer des tables Gold avec des KPIs business exploitables :
              chiffre d&apos;affaires par catégorie, top clients, tendances
              quotidiennes et répartition des modes de paiement.
            </p>

            <InfoBox type="info" title="Architecture Medallion — Gold">
              La couche Gold contient les données agrégées et prêtes à
              l&apos;emploi pour les dashboards et rapports. Chaque table
              Gold répond à une question business précise.
            </InfoBox>

            <h3 className="text-lg font-semibold text-[#1b3a4b]">
              📝 Instructions
            </h3>
            <ol className="list-decimal list-inside space-y-2 text-gray-700">
              <li>
                Créez <code>gold_revenue_by_category</code> : CA, nombre de
                commandes et clients uniques par catégorie.
              </li>
              <li>
                Créez <code>gold_top_customers</code> : classement des
                meilleurs clients avec leur total dépensé.
              </li>
              <li>
                Créez <code>gold_daily_trends</code> : tendances
                quotidiennes (CA, commandes, panier moyen).
              </li>
              <li>
                Créez <code>gold_payment_methods</code> : répartition par
                mode de paiement avec pourcentage.
              </li>
            </ol>

            <SolutionToggle id="sol-etape4">
              <p className="text-sm font-semibold text-gray-600 mb-2">
                Code complet — Couche Gold :
              </p>
              <CodeBlock
                language="sql"
                title="Gold KPI 1 : CA par catégorie de produit"
                code={`CREATE OR REPLACE TABLE ecommerce.gold_revenue_by_category AS
SELECT
  category,
  COUNT(DISTINCT order_id) AS total_orders,
  SUM(total_amount) AS total_revenue,
  AVG(total_amount) AS avg_order_value,
  COUNT(DISTINCT customer_id) AS unique_customers
FROM ecommerce.silver_orders_enriched
GROUP BY category
ORDER BY total_revenue DESC;`}
              />
              <CodeBlock
                language="sql"
                title="Gold KPI 2 : Top clients"
                code={`CREATE OR REPLACE TABLE ecommerce.gold_top_customers AS
SELECT
  customer_id, name, city, segment,
  COUNT(order_id) AS nb_orders,
  SUM(total_amount) AS total_spent,
  AVG(total_amount) AS avg_order_value,
  MIN(order_date) AS first_order,
  MAX(order_date) AS last_order
FROM ecommerce.silver_orders_enriched
GROUP BY customer_id, name, city, segment
ORDER BY total_spent DESC;`}
              />
              <CodeBlock
                language="sql"
                title="Gold KPI 3 : Tendances quotidiennes"
                code={`CREATE OR REPLACE TABLE ecommerce.gold_daily_trends AS
SELECT
  order_date,
  COUNT(order_id) AS nb_orders,
  SUM(total_amount) AS daily_revenue,
  COUNT(DISTINCT customer_id) AS unique_customers,
  AVG(total_amount) AS avg_order_value
FROM ecommerce.silver_orders_enriched
GROUP BY order_date
ORDER BY order_date;`}
              />
              <CodeBlock
                language="sql"
                title="Gold KPI 4 : Répartition par mode de paiement"
                code={`CREATE OR REPLACE TABLE ecommerce.gold_payment_methods AS
SELECT
  payment_method,
  COUNT(*) AS nb_transactions,
  SUM(total_amount) AS total_amount,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM ecommerce.silver_orders_enriched
GROUP BY payment_method;`}
              />
            </SolutionToggle>

            <InfoBox type="tip" title="Fonctions fenêtrées">
              La fonction <code>SUM(COUNT(*)) OVER ()</code> est une fonction
              fenêtrée qui calcule le total global, permettant de calculer le
              pourcentage de chaque mode de paiement.
            </InfoBox>
          </div>
        </section>

        {/* ====================== ÉTAPE 5 ====================== */}
        <section className="mb-14">
          <div className="flex items-center gap-3 mb-2">
            <span className="w-8 h-8 flex items-center justify-center bg-[#ff3621] text-white text-sm font-bold rounded-full">
              5
            </span>
            <h2 className="text-2xl font-bold text-[#1b3a4b]">
              Validation &amp; Qualité
            </h2>
          </div>
          <div className="flex items-center gap-3 mb-5 ml-11">
            <span className="text-xs font-medium bg-gray-100 text-gray-600 px-2.5 py-1 rounded-full">
              ⏱ 30 min
            </span>
          </div>

          <div className="ml-11 space-y-4">
            <h3 className="text-lg font-semibold text-[#1b3a4b]">
              📖 Objectif
            </h3>
            <p className="text-gray-700 leading-relaxed">
              Valider l&apos;intégrité du pipeline en vérifiant le nombre de
              lignes à chaque couche, l&apos;absence de doublons, et la
              cohérence des données.
            </p>

            <InfoBox type="important" title="Qualité des données">
              La validation est une étape cruciale. En production, ces
              vérifications seraient automatisées avec des{" "}
              <strong>expectations</strong> dans Delta Live Tables ou des
              tests unitaires.
            </InfoBox>

            <h3 className="text-lg font-semibold text-[#1b3a4b]">
              📝 Instructions
            </h3>
            <ol className="list-decimal list-inside space-y-2 text-gray-700">
              <li>
                Comparez le nombre de lignes entre Bronze, Silver et Silver
                Enriched.
              </li>
              <li>
                Vérifiez qu&apos;il n&apos;y a pas de doublons dans{" "}
                <code>silver_orders</code>.
              </li>
              <li>
                Vérifiez la cohérence des montants dans les tables Gold.
              </li>
            </ol>

            <SolutionToggle id="sol-etape5">
              <p className="text-sm font-semibold text-gray-600 mb-2">
                Code complet — Validation :
              </p>
              <CodeBlock
                language="sql"
                title="Comptage par couche"
                code={`-- Vérifications qualité : nombre de lignes par couche
SELECT 'Bronze Orders' AS layer, COUNT(*) AS cnt FROM ecommerce.bronze_orders
UNION ALL
SELECT 'Silver Orders', COUNT(*) FROM ecommerce.silver_orders
UNION ALL
SELECT 'Silver Enriched', COUNT(*) FROM ecommerce.silver_orders_enriched;`}
              />
              <CodeBlock
                language="sql"
                title="Vérification des doublons"
                code={`-- Vérifier qu'il n'y a pas de doublons
SELECT order_id, COUNT(*) AS cnt
FROM ecommerce.silver_orders
GROUP BY order_id
HAVING cnt > 1;

-- Résultat attendu : aucune ligne (0 doublon)`}
              />
              <CodeBlock
                language="sql"
                title="Vérification de cohérence"
                code={`-- Vérifier que le CA total Gold correspond au Silver
SELECT SUM(total_revenue) AS gold_total
FROM ecommerce.gold_revenue_by_category;

SELECT SUM(total_amount) AS silver_total
FROM ecommerce.silver_orders_enriched;

-- Les deux montants doivent être identiques`}
              />
            </SolutionToggle>

            <InfoBox type="tip" title="Bonne pratique">
              En production, utilisez les <strong>constraints</strong> de
              Delta Live Tables (<code>CONSTRAINT valid_order EXPECT (order_id IS NOT NULL)</code>)
              pour automatiser ces vérifications.
            </InfoBox>
          </div>
        </section>

        {/* Résumé */}
        <section className="mb-14">
          <div className="bg-gradient-to-r from-[#1b3a4b] to-[#2d5f7a] rounded-xl p-6 text-white">
            <h2 className="text-xl font-bold mb-4">
              🎓 Récapitulatif du projet
            </h2>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              <div className="bg-white/10 rounded-lg p-4">
                <div className="text-2xl mb-2">🥉</div>
                <h3 className="font-bold mb-1">Bronze</h3>
                <p className="text-sm text-white/80">
                  3 tables brutes (commandes, clients, produits) avec
                  métadonnées d&apos;ingestion.
                </p>
              </div>
              <div className="bg-white/10 rounded-lg p-4">
                <div className="text-2xl mb-2">🥈</div>
                <h3 className="font-bold mb-1">Silver</h3>
                <p className="text-sm text-white/80">
                  Données nettoyées, dédupliquées et enrichies par jointures.
                </p>
              </div>
              <div className="bg-white/10 rounded-lg p-4">
                <div className="text-2xl mb-2">🥇</div>
                <h3 className="font-bold mb-1">Gold</h3>
                <p className="text-sm text-white/80">
                  4 tables de KPIs : CA par catégorie, top clients, tendances
                  quotidiennes, paiements.
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
            href="/programme"
            className="inline-flex items-center gap-2 px-5 py-2.5 rounded-lg bg-[#ff3621] text-white font-semibold hover:bg-[#e02e1a] transition-colors"
          >
            📅 Voir le programme →
          </Link>
        </div>
      </div>
    </div>
  );
}
