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
    question: "Dans l'architecture Medallion, quelle couche contient les données brutes ?",
    options: [
      "Gold",
      "Silver",
      "Bronze",
      "Raw"
    ],
    correctIndex: 2,
    explanation: "La couche Bronze contient les données brutes telles qu'elles ont été ingérées, sans transformation. Elle sert de zone d'atterrissage (landing zone) et conserve l'historique complet des données sources."
  },
  {
    question: "Quelles opérations sont typiques de la couche Silver ?",
    options: [
      "Ingestion brute",
      "Agrégations métier",
      "Nettoyage, déduplication, jointures et enrichissement",
      "Visualisation"
    ],
    correctIndex: 2,
    explanation: "La couche Silver est la couche de nettoyage et d'enrichissement. On y applique le filtrage des données invalides, la déduplication, les jointures entre différentes sources, et la normalisation des types de données."
  },
  {
    question: "Pourquoi utiliser le streaming entre chaque couche ?",
    options: [
      "C'est obligatoire",
      "Pour un traitement incrémental et efficace sans retraiter toutes les données",
      "Pour réduire la taille des fichiers",
      "Pour le debug"
    ],
    correctIndex: 1,
    explanation: "Le streaming entre les couches permet un traitement incrémental : seules les nouvelles données sont traitées à chaque exécution, ce qui est beaucoup plus efficace que de retraiter l'intégralité des données. Ce n'est pas obligatoire mais fortement recommandé."
  },
  {
    question: "Quel output mode est souvent utilisé pour les tables Gold avec agrégation ?",
    options: [
      "append",
      "complete",
      "update",
      "overwrite"
    ],
    correctIndex: 1,
    explanation: "Le mode 'complete' est souvent utilisé pour les tables Gold car elles contiennent typiquement des agrégations métier (KPIs, métriques) qui doivent être recalculées intégralement. Le mode complete réécrit toute la table de résultats à chaque batch."
  },
  {
    question: "Combien de couches y a-t-il dans l'architecture Medallion standard ?",
    options: [
      "2",
      "3 (Bronze, Silver, Gold)",
      "4",
      "5"
    ],
    correctIndex: 1,
    explanation: "L'architecture Medallion standard comporte 3 couches : Bronze (données brutes), Silver (données nettoyées et enrichies), et Gold (données agrégées et optimisées pour la consommation métier). Des couches supplémentaires peuvent être ajoutées selon les besoins."
  }
];

const exercises: LessonExercise[] = [
  {
    id: "dessiner-architecture-medallion",
    title: "Dessiner une architecture Medallion",
    description: "Concevez l'architecture Medallion pour une application d'analytique de réseaux sociaux.",
    difficulty: "facile",
    type: "reflexion",
    prompt: "Pour une application d'analytique de réseaux sociaux (posts, likes, commentaires, profils utilisateurs), décrivez ce que contiendrait chaque couche (Bronze, Silver, Gold). Quelles données ? Quelles transformations ?",
    hints: [
      "En Bronze, pensez aux données brutes provenant des APIs : posts JSON, events de likes, logs de commentaires",
      "En Silver, pensez au nettoyage : déduplication des events, jointure posts + profils, filtrage du spam",
      "En Gold, pensez aux métriques métier : engagement par post, top utilisateurs, tendances par jour"
    ],
    solution: {
      code: `# BRONZE (Données brutes)\n# - posts_raw : JSON bruts des posts (id, user_id, content, timestamp)\n# - likes_raw : événements de likes (user_id, post_id, timestamp)\n# - comments_raw : commentaires bruts (id, user_id, post_id, text)\n# - users_raw : profils utilisateurs depuis l'API\n# → Aucune transformation, conservation de _rescued_data\n\n# SILVER (Données nettoyées et enrichies)\n# - posts_clean : déduplication, filtrage spam, parsing des hashtags\n# - interactions : jointure likes + comments + posts\n# - users_enriched : jointure profils + statistiques d'activité\n# → Transformations : dédup, jointures, filtrage, typage\n\n# GOLD (Métriques métier)\n# - engagement_metrics : taux d'engagement par post (likes + comments / vues)\n# - top_users_daily : classement des utilisateurs les plus actifs par jour\n# - trending_hashtags : hashtags tendances par heure/jour\n# - content_performance : performance des types de contenu\n# → Agrégations et KPIs prêts pour les dashboards`,
      language: "python",
      explanation: "Chaque couche a un rôle précis : Bronze conserve les données brutes pour la traçabilité, Silver les enrichit et les nettoie pour l'analyse, Gold produit les métriques métier directement consommables par les équipes data et les dashboards."
    }
  },
  {
    id: "implementer-pipeline-multi-hop",
    title: "Implémenter un pipeline Multi-Hop",
    description: "Écrivez le code complet pour un pipeline Bronze → Silver → Gold en streaming.",
    difficulty: "difficile",
    type: "code",
    prompt: "Implémentez un pipeline Multi-Hop complet pour des données de ventes : Bronze (ingestion JSON brut), Silver (nettoyage + jointure produits), Gold (chiffre d'affaires par catégorie par jour).",
    hints: [
      "Chaque couche lit en streaming depuis la couche précédente avec spark.readStream",
      "En Silver, utilisez des jointures avec des tables de référence (produits) qui peuvent être lues en batch",
      "En Gold, utilisez une agrégation GROUP BY avec le mode complete ou un merge Delta"
    ],
    solution: {
      code: `# ===== BRONZE : Ingestion brute =====\ndf_bronze = (spark.readStream\n  .format("cloudFiles")\n  .option("cloudFiles.format", "json")\n  .option("cloudFiles.inferColumnTypes", "true")\n  .option("cloudFiles.schemaLocation", "/mnt/checkpoints/sales_schema")\n  .load("/mnt/data/raw/sales/")\n)\n\n(df_bronze.writeStream\n  .format("delta")\n  .outputMode("append")\n  .option("checkpointLocation", "/mnt/checkpoints/bronze_sales")\n  .trigger(availableNow=True)\n  .toTable("bronze_sales")\n)\n\n# ===== SILVER : Nettoyage + Enrichissement =====\ndf_products = spark.read.table("ref_products")  # table de référence\n\ndf_silver = (spark.readStream\n  .table("bronze_sales")\n  .filter("amount > 0 AND product_id IS NOT NULL")\n  .dropDuplicates(["sale_id"])\n  .join(df_products, "product_id", "left")\n  .select("sale_id", "product_id", "category", "amount", "sale_date")\n)\n\n(df_silver.writeStream\n  .format("delta")\n  .outputMode("append")\n  .option("checkpointLocation", "/mnt/checkpoints/silver_sales")\n  .trigger(availableNow=True)\n  .toTable("silver_sales")\n)\n\n# ===== GOLD : Agrégations métier =====\nfrom pyspark.sql.functions import sum, col, to_date\n\ndf_gold = (spark.readStream\n  .table("silver_sales")\n  .groupBy(\n    to_date("sale_date").alias("day"),\n    "category"\n  )\n  .agg(sum("amount").alias("daily_revenue"))\n)\n\n(df_gold.writeStream\n  .format("delta")\n  .outputMode("complete")\n  .option("checkpointLocation", "/mnt/checkpoints/gold_sales")\n  .trigger(availableNow=True)\n  .toTable("gold_daily_revenue")\n)`,
      language: "python",
      explanation: "Ce pipeline illustre le pattern Multi-Hop classique : Bronze ingère les données brutes via Auto Loader, Silver les nettoie (filtrage, déduplication) et les enrichit (jointure produits), Gold produit les agrégations métier. Chaque couche utilise le streaming pour un traitement incrémental efficace."
    }
  }
];

export default function ArchitectureMultiHopPage() {
  return (
    <div className="flex min-h-[calc(100vh-4rem)]">
      <Sidebar currentPath="/modules/3-3-architecture-multi-hop" />

      <main className="flex-1 overflow-y-auto">
        <div className="max-w-4xl mx-auto px-6 py-10 lg:px-10">
          {/* Header */}
          <div className="mb-10">
            <div className="flex items-center gap-3 mb-3">
              <span className="inline-flex items-center px-3 py-1 rounded-full text-xs font-semibold bg-blue-100 text-blue-800">
                Module 3
              </span>
              <span className="text-sm text-[var(--color-text-light)]">
                Leçon 3.3
              </span>
            </div>
            <h1 className="text-3xl font-bold text-[var(--color-text)] mb-3">
              Architecture Multi-Hop (Medallion)
            </h1>
            <p className="text-lg text-[var(--color-text-light)] leading-relaxed">
              Comprenez l&apos;architecture Medallion (Bronze, Silver, Gold),
              le modèle de référence pour organiser les données dans un
              Lakehouse. Apprenez à construire un pipeline de données
              incrémental à travers les différentes couches de qualité.
            </p>
          </div>

          {/* Content */}
          <section className="space-y-8">
            {/* Introduction */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Qu&apos;est-ce que l&apos;architecture Medallion ?
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                L&apos;architecture <strong>Medallion</strong> (aussi appelée
                architecture <strong>Multi-Hop</strong>) est un modèle de
                conception de données qui organise les données en{" "}
                <strong>trois couches</strong> de qualité croissante :{" "}
                <strong>Bronze</strong>, <strong>Silver</strong> et{" "}
                <strong>Gold</strong>. Chaque couche représente un niveau de
                transformation et de qualité des données.
              </p>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Ce modèle est au cœur de la philosophie{" "}
                <strong>Databricks Lakehouse</strong> et permet de garantir une
                progression logique de la qualité des données, de
                l&apos;ingestion brute jusqu&apos;aux agrégations métier prêtes
                pour la BI et le Machine Learning.
              </p>
              <InfoBox type="tip" title="Sujet central de l'examen">
                <p>
                  L&apos;architecture Medallion est un sujet{" "}
                  <strong>incontournable</strong> de la certification Databricks
                  Data Engineer Associate. Vous devez maîtriser le rôle de
                  chaque couche, les types de transformations appliquées et
                  comment les données transitent entre les couches.
                </p>
              </InfoBox>
            </div>

            {/* Architecture diagram */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Vue d&apos;ensemble de l&apos;architecture
              </h2>
              <div className="flex flex-col md:flex-row items-center gap-3 my-6">
                <div className="flex-1 bg-gray-100 border-2 border-gray-300 rounded-xl p-4 text-center">
                  <div className="text-2xl mb-2">☁️</div>
                  <p className="font-semibold text-[var(--color-text)]">
                    Cloud Storage
                  </p>
                  <p className="text-xs text-[var(--color-text-light)]">
                    Fichiers bruts (JSON, CSV, Parquet...)
                  </p>
                </div>
                <div className="text-2xl text-gray-400 hidden md:block">→</div>
                <div className="text-2xl text-gray-400 md:hidden">↓</div>
                <div className="flex-1 bg-amber-50 border-2 border-amber-300 rounded-xl p-4 text-center">
                  <div className="text-2xl mb-2">🥉</div>
                  <p className="font-semibold text-amber-800">Bronze</p>
                  <p className="text-xs text-amber-700">
                    Données brutes, non transformées
                  </p>
                </div>
                <div className="text-2xl text-gray-400 hidden md:block">→</div>
                <div className="text-2xl text-gray-400 md:hidden">↓</div>
                <div className="flex-1 bg-slate-100 border-2 border-slate-300 rounded-xl p-4 text-center">
                  <div className="text-2xl mb-2">🥈</div>
                  <p className="font-semibold text-slate-800">Silver</p>
                  <p className="text-xs text-slate-700">
                    Données nettoyées, filtrées
                  </p>
                </div>
                <div className="text-2xl text-gray-400 hidden md:block">→</div>
                <div className="text-2xl text-gray-400 md:hidden">↓</div>
                <div className="flex-1 bg-yellow-50 border-2 border-yellow-400 rounded-xl p-4 text-center">
                  <div className="text-2xl mb-2">🥇</div>
                  <p className="font-semibold text-yellow-800">Gold</p>
                  <p className="text-xs text-yellow-700">
                    Agrégations métier, prêt pour BI/ML
                  </p>
                </div>
              </div>
            </div>

            {/* Bronze layer */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Couche Bronze — Données brutes
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                La couche <strong>Bronze</strong> est le point d&apos;entrée des
                données dans le Lakehouse. Elle contient les données{" "}
                <strong>brutes</strong>, telles qu&apos;elles arrivent de la
                source, sans aucune transformation ni nettoyage.
              </p>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Caractéristiques principales :
              </p>
              <ul className="list-disc list-inside space-y-2 text-[var(--color-text-light)] mb-4">
                <li>
                  <strong>Aucune transformation</strong> : Les données sont
                  stockées exactement comme reçues.
                </li>
                <li>
                  <strong>Append-only</strong> : Les données sont uniquement
                  ajoutées, jamais modifiées ou supprimées.
                </li>
                <li>
                  <strong>Métadonnées d&apos;ingestion</strong> : On ajoute
                  souvent des colonnes comme la date d&apos;ingestion, le nom
                  du fichier source, etc.
                </li>
                <li>
                  <strong>Format Delta</strong> : Stocké en format Delta Lake
                  pour bénéficier du versioning et des transactions ACID.
                </li>
              </ul>
              <CodeBlock
                language="python"
                title="Bronze : Ingestion brute avec Auto Loader"
                code={`from pyspark.sql.functions import current_timestamp, input_file_name

# Bronze : Ingestion brute sans transformation
(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/schema/bronze")
    .load("/raw/data")
    .withColumn("ingestion_time", current_timestamp())
    .withColumn("source_file", input_file_name())
    .writeStream
    .option("checkpointLocation", "/checkpoint/bronze")
    .outputMode("append")
    .table("bronze_table")
)`}
              />
            </div>

            {/* Silver layer */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Couche Silver — Données nettoyées
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                La couche <strong>Silver</strong> contient des données{" "}
                <strong>nettoyées, filtrées et enrichies</strong>. C&apos;est
                ici que l&apos;on applique les transformations de qualité pour
                rendre les données exploitables.
              </p>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Transformations typiques :
              </p>
              <ul className="list-disc list-inside space-y-2 text-[var(--color-text-light)] mb-4">
                <li>
                  <strong>Filtrage</strong> : Suppression des enregistrements
                  invalides ou incomplets (NULL, valeurs aberrantes).
                </li>
                <li>
                  <strong>Déduplication</strong> : Élimination des doublons via{" "}
                  <code className="bg-gray-100 px-1.5 py-0.5 rounded text-sm font-mono">
                    dropDuplicates()
                  </code>
                  .
                </li>
                <li>
                  <strong>Casting de types</strong> : Conversion des types de
                  données (string → timestamp, string → integer).
                </li>
                <li>
                  <strong>Jointures</strong> : Enrichissement des données avec
                  des tables de référence (dimensions).
                </li>
                <li>
                  <strong>Normalisation</strong> : Standardisation des formats
                  (dates, noms, codes).
                </li>
              </ul>
              <CodeBlock
                language="python"
                title="Silver : Nettoyage et enrichissement"
                code={`from pyspark.sql.functions import col

# Silver : Lecture depuis Bronze, nettoyage et enrichissement
(spark.readStream
    .table("bronze_table")
    .filter("status IS NOT NULL")
    .filter(col("amount") > 0)
    .dropDuplicates(["id"])
    .withColumn("amount", col("amount").cast("double"))
    .writeStream
    .option("checkpointLocation", "/checkpoint/silver")
    .outputMode("append")
    .table("silver_table")
)`}
              />
            </div>

            {/* Gold layer */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Couche Gold — Agrégations métier
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                La couche <strong>Gold</strong> contient des données{" "}
                <strong>agrégées et orientées métier</strong>, prêtes à être
                consommées par les outils de BI (Power BI, Tableau) ou les
                modèles de Machine Learning.
              </p>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Caractéristiques principales :
              </p>
              <ul className="list-disc list-inside space-y-2 text-[var(--color-text-light)] mb-4">
                <li>
                  <strong>Agrégations</strong> : Sommes, moyennes, comptages par
                  dimensions métier (catégorie, région, période).
                </li>
                <li>
                  <strong>Tables orientées métier</strong> : Modèle en étoile
                  (star schema) avec des tables de faits et de dimensions.
                </li>
                <li>
                  <strong>Performances optimisées</strong> : Tables pré-calculées
                  pour des requêtes rapides.
                </li>
                <li>
                  <strong>Accès contrôlé</strong> : Souvent les seules tables
                  exposées aux analystes et data scientists.
                </li>
              </ul>
              <CodeBlock
                language="python"
                title="Gold : Agrégation métier"
                code={`from pyspark.sql.functions import count, sum, avg

# Gold : Agrégation par catégorie depuis Silver
(spark.readStream
    .table("silver_table")
    .groupBy("category")
    .agg(
        count("*").alias("total_transactions"),
        sum("amount").alias("montant_total"),
        avg("amount").alias("montant_moyen")
    )
    .writeStream
    .option("checkpointLocation", "/checkpoint/gold")
    .outputMode("complete")
    .table("gold_table")
)`}
              />
              <InfoBox type="info" title="outputMode(&quot;complete&quot;) pour les agrégations Gold">
                <p>
                  Les tables Gold contiennent souvent des agrégations (GROUP BY),
                  ce qui nécessite le mode de sortie{" "}
                  <strong>complete</strong>. En mode complete, la table entière
                  est réécrite à chaque micro-batch pour refléter les agrégations
                  mises à jour. C&apos;est le seul mode compatible avec les
                  agrégations sans watermark.
                </p>
              </InfoBox>
            </div>

            {/* Streaming between layers */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Streaming entre les couches
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                L&apos;un des principes fondamentaux de l&apos;architecture
                Medallion est que chaque transition entre couches peut être
                implémentée comme un <strong>flux streaming</strong>. Cela
                garantit un traitement <strong>incrémental</strong> : seules les
                nouvelles données sont traitées à chaque exécution.
              </p>
              <InfoBox type="important" title="Streaming pour chaque hop">
                <p>
                  Utilisez <strong>Structured Streaming</strong> (readStream /
                  writeStream) pour connecter chaque couche de
                  l&apos;architecture. Cela garantit un traitement incrémental
                  efficace. Chaque hop (Bronze→Silver, Silver→Gold) est un flux
                  streaming indépendant avec son propre checkpoint. Le trigger{" "}
                  <code className="bg-gray-100 px-1.5 py-0.5 rounded text-sm font-mono">
                    availableNow=True
                  </code>{" "}
                  est idéal pour les pipelines planifiés.
                </p>
              </InfoBox>
              <CodeBlock
                language="python"
                title="Pipeline complet Multi-Hop"
                code={`# ============================================
# Pipeline complet : Bronze → Silver → Gold
# ============================================

# 1. BRONZE : Ingestion brute depuis le cloud storage
bronze_query = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/schema/bronze")
    .load("/raw/orders")
    .writeStream
    .trigger(availableNow=True)
    .option("checkpointLocation", "/checkpoint/bronze")
    .outputMode("append")
    .table("orders_bronze")
    .awaitTermination()
)

# 2. SILVER : Nettoyage et enrichissement
silver_query = (spark.readStream
    .table("orders_bronze")
    .filter("order_id IS NOT NULL AND amount > 0")
    .dropDuplicates(["order_id"])
    .join(spark.table("dim_customers"), "customer_id", "left")
    .writeStream
    .trigger(availableNow=True)
    .option("checkpointLocation", "/checkpoint/silver")
    .outputMode("append")
    .table("orders_silver")
    .awaitTermination()
)

# 3. GOLD : Agrégation métier
gold_query = (spark.readStream
    .table("orders_silver")
    .groupBy("region", "product_category")
    .agg(
        count("*").alias("nb_commandes"),
        sum("amount").alias("ca_total"),
        avg("amount").alias("panier_moyen")
    )
    .writeStream
    .trigger(availableNow=True)
    .option("checkpointLocation", "/checkpoint/gold")
    .outputMode("complete")
    .table("orders_gold")
    .awaitTermination()
)`}
              />
            </div>

            {/* Quality guarantees */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Garanties de qualité par couche
              </h2>
              <div className="overflow-x-auto my-6">
                <table className="w-full border-collapse border border-gray-200 text-sm">
                  <thead>
                    <tr className="bg-gray-50">
                      <th className="border border-gray-200 px-4 py-3 text-left font-semibold text-[var(--color-text)]">
                        Critère
                      </th>
                      <th className="border border-gray-200 px-4 py-3 text-left font-semibold text-[var(--color-text)]">
                        🥉 Bronze
                      </th>
                      <th className="border border-gray-200 px-4 py-3 text-left font-semibold text-[var(--color-text)]">
                        🥈 Silver
                      </th>
                      <th className="border border-gray-200 px-4 py-3 text-left font-semibold text-[var(--color-text)]">
                        🥇 Gold
                      </th>
                    </tr>
                  </thead>
                  <tbody className="text-[var(--color-text-light)]">
                    <tr>
                      <td className="border border-gray-200 px-4 py-3 font-medium">
                        Qualité des données
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Brute, non validée
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Nettoyée, validée
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Agrégée, fiable
                      </td>
                    </tr>
                    <tr className="bg-gray-50">
                      <td className="border border-gray-200 px-4 py-3 font-medium">
                        Objectif
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Préserver la donnée brute
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Préparer des données exploitables
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Fournir des KPIs métier
                      </td>
                    </tr>
                    <tr>
                      <td className="border border-gray-200 px-4 py-3 font-medium">
                        Opérations typiques
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Ingestion, ajout de métadonnées
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Filtrage, déduplication, jointures, casting
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        GROUP BY, agrégations, pivots
                      </td>
                    </tr>
                    <tr className="bg-gray-50">
                      <td className="border border-gray-200 px-4 py-3 font-medium">
                        Mode de sortie
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Append
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Append
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Complete
                      </td>
                    </tr>
                    <tr>
                      <td className="border border-gray-200 px-4 py-3 font-medium">
                        Consommateurs
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Data Engineers
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Data Analysts, Data Scientists
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        BI, Dashboards, ML
                      </td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </div>

            {/* Récapitulatif */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Points clés à retenir
              </h2>
              <ul className="list-disc list-inside space-y-3 text-[var(--color-text-light)]">
                <li>
                  L&apos;architecture Medallion organise les données en trois
                  couches : <strong>Bronze</strong> (brut),{" "}
                  <strong>Silver</strong> (nettoyé), <strong>Gold</strong>{" "}
                  (agrégé).
                </li>
                <li>
                  Chaque couche augmente la <strong>qualité</strong> et la{" "}
                  <strong>valeur métier</strong> des données.
                </li>
                <li>
                  Utilisez <strong>Structured Streaming</strong> pour connecter
                  les couches et garantir un traitement incrémental.
                </li>
                <li>
                  La couche Bronze n&apos;applique <strong>aucune transformation</strong>{" "}
                  et fonctionne en mode <strong>append</strong>.
                </li>
                <li>
                  La couche Silver applique des{" "}
                  <strong>transformations de qualité</strong> (filtrage,
                  déduplication, jointures).
                </li>
                <li>
                  La couche Gold produit des{" "}
                  <strong>agrégations métier</strong> et utilise souvent le mode{" "}
                  <strong>complete</strong>.
                </li>
                <li>
                  Chaque flux streaming doit avoir son propre{" "}
                  <strong>checkpoint</strong>.
                </li>
              </ul>
            </div>
          </section>

          {/* Quiz */}
          <Quiz
            lessonSlug="3-3-architecture-multi-hop"
            title="Quiz — Architecture Multi-Hop (Medallion)"
            questions={quizQuestions}
          />

          {/* Exercices */}
          <LessonExercises
            lessonSlug="3-3-architecture-multi-hop"
            exercises={exercises}
          />

          {/* Bouton de complétion */}
          <LessonCompleteButton lessonSlug="3-3-architecture-multi-hop" />

          {/* Navigation */}
          <div className="flex flex-col sm:flex-row justify-between gap-4 mt-12 pt-8 border-t border-[var(--color-border)]">
            <Link
              href="/modules/3-2-auto-loader"
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
              Leçon précédente : Auto Loader
            </Link>
            <Link
              href="/modules/4-1-delta-live-tables"
              className="inline-flex items-center gap-2 px-5 py-2.5 bg-[#ff3621] text-white rounded-lg font-medium hover:bg-[#e02e1a] transition-colors"
            >
              Leçon suivante : Delta Live Tables
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
