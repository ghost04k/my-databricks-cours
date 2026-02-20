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

export default function DeltaLakeAvancePage() {
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
            <span className="text-sm text-white/70">⏱ 3 heures</span>
            <span className="text-sm text-white/70">📘 Delta Lake</span>
          </div>
          <h1 className="text-3xl lg:text-4xl font-extrabold mb-3">
            🔺 Exercices : Delta Lake Avancé
          </h1>
          <p className="text-lg text-white/80 max-w-2xl leading-relaxed">
            4 exercices avancés pour maîtriser les fonctionnalités clés de Delta
            Lake : Time Travel, optimisation, Change Data Feed et évolution de
            schéma.
          </p>
        </div>
      </div>

      {/* Contenu */}
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
                Time Travel{" "}
                <span className="text-gray-400">(45 min)</span>
              </span>
            </li>
            <li className="flex items-center gap-2">
              <span className="w-6 h-6 flex items-center justify-center bg-[#1b3a4b] text-white text-xs font-bold rounded-full">
                2
              </span>
              <span>
                Optimisation Delta{" "}
                <span className="text-gray-400">(45 min)</span>
              </span>
            </li>
            <li className="flex items-center gap-2">
              <span className="w-6 h-6 flex items-center justify-center bg-[#1b3a4b] text-white text-xs font-bold rounded-full">
                3
              </span>
              <span>
                Change Data Feed{" "}
                <span className="text-gray-400">(45 min)</span>
              </span>
            </li>
            <li className="flex items-center gap-2">
              <span className="w-6 h-6 flex items-center justify-center bg-[#1b3a4b] text-white text-xs font-bold rounded-full">
                4
              </span>
              <span>
                Schéma Evolution &amp; Enforcement{" "}
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
              Time Travel
            </h2>
          </div>
          <div className="flex items-center gap-3 mb-5 ml-11">
            <span className="text-xs font-medium bg-gray-100 text-gray-600 px-2.5 py-1 rounded-full">
              ⏱ 45 min
            </span>
            <span className="text-xs font-medium bg-red-100 text-red-700 px-2.5 py-1 rounded-full">
              Avancé
            </span>
          </div>

          <div className="ml-11 space-y-4">
            <h3 className="text-lg font-semibold text-[#1b3a4b]">
              📖 Contexte
            </h3>
            <p className="text-gray-700 leading-relaxed">
              Delta Lake conserve un journal de transactions (transaction log)
              qui enregistre chaque modification apportée à une table. Le{" "}
              <strong>Time Travel</strong> vous permet de consulter ou restaurer
              des versions antérieures de vos données. Cette fonctionnalité est
              essentielle pour l&apos;audit, le débogage et la récupération de
              données supprimées accidentellement.
            </p>

            <h3 className="text-lg font-semibold text-[#1b3a4b]">
              📝 Instructions
            </h3>
            <ol className="list-decimal list-inside space-y-2 text-gray-700">
              <li>
                Créez une table <code>products</code> et effectuez plusieurs
                modifications (INSERT, UPDATE, DELETE).
              </li>
              <li>
                Utilisez <code>DESCRIBE HISTORY</code> pour observer
                l&apos;historique des versions.
              </li>
              <li>
                Accédez à une version précédente via{" "}
                <code>VERSION AS OF</code> et <code>TIMESTAMP AS OF</code>.
              </li>
              <li>
                Restaurez une version antérieure avec{" "}
                <code>RESTORE TABLE</code>.
              </li>
            </ol>

            <InfoBox type="tip" title="Astuce">
              Chaque opération sur une table Delta crée une nouvelle version.
              Vous pouvez remonter jusqu&apos;à 30 jours dans le passé par
              défaut, tant que les fichiers n&apos;ont pas été nettoyés par{" "}
              <code>VACUUM</code>.
            </InfoBox>

            <SolutionToggle id="sol-1">
              <p className="text-sm text-gray-700 font-semibold mb-2">
                Étape 1 : Créer la table et effectuer des modifications
              </p>
              <CodeBlock
                language="sql"
                title="Création et modifications"
                code={`-- Créer une table et faire plusieurs modifications
CREATE OR REPLACE TABLE products (
  id INT, name STRING, price DOUBLE, category STRING
);

INSERT INTO products VALUES (1, 'Laptop', 999.99, 'Electronics');
INSERT INTO products VALUES (2, 'Phone', 599.99, 'Electronics');
UPDATE products SET price = 899.99 WHERE id = 1;
DELETE FROM products WHERE id = 2;`}
              />

              <p className="text-sm text-gray-700 font-semibold mb-2">
                Étape 2 : Consulter l&apos;historique
              </p>
              <CodeBlock
                language="sql"
                title="Historique des versions"
                code={`-- Voir l'historique complet de la table
DESCRIBE HISTORY products;`}
              />
              <p className="text-sm text-gray-600">
                Cette commande affiche toutes les versions avec la date, le type
                d&apos;opération (WRITE, UPDATE, DELETE…), l&apos;utilisateur
                et les métriques associées.
              </p>

              <p className="text-sm text-gray-700 font-semibold mb-2">
                Étape 3 : Accéder aux versions précédentes
              </p>
              <CodeBlock
                language="sql"
                title="Time Travel par version"
                code={`-- Time Travel : voir la version 1
SELECT * FROM products VERSION AS OF 1;

-- Time Travel : voir à un timestamp précis
SELECT * FROM products TIMESTAMP AS OF '2024-01-15T10:00:00';`}
              />
              <p className="text-sm text-gray-600">
                <code>VERSION AS OF</code> utilise le numéro de version (visible
                dans <code>DESCRIBE HISTORY</code>).{" "}
                <code>TIMESTAMP AS OF</code> utilise une date/heure pour
                retrouver la version la plus proche.
              </p>

              <p className="text-sm text-gray-700 font-semibold mb-2">
                Étape 4 : Restaurer une version
              </p>
              <CodeBlock
                language="sql"
                title="Restauration"
                code={`-- Restaurer une version précédente
RESTORE TABLE products TO VERSION AS OF 2;`}
              />
              <p className="text-sm text-gray-600">
                <strong>RESTORE TABLE</strong> ne supprime pas les versions
                suivantes : elle crée une{" "}
                <em>nouvelle</em> version dont le contenu est identique à la
                version ciblée. L&apos;historique est donc préservé.
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
              Optimisation Delta
            </h2>
          </div>
          <div className="flex items-center gap-3 mb-5 ml-11">
            <span className="text-xs font-medium bg-gray-100 text-gray-600 px-2.5 py-1 rounded-full">
              ⏱ 45 min
            </span>
            <span className="text-xs font-medium bg-red-100 text-red-700 px-2.5 py-1 rounded-full">
              Avancé
            </span>
          </div>

          <div className="ml-11 space-y-4">
            <h3 className="text-lg font-semibold text-[#1b3a4b]">
              📖 Contexte
            </h3>
            <p className="text-gray-700 leading-relaxed">
              Au fil des écritures, les tables Delta peuvent accumuler de
              nombreux petits fichiers (small files problem), ce qui dégrade les
              performances des requêtes. Delta Lake propose des commandes
              d&apos;optimisation :{" "}
              <strong>OPTIMIZE</strong> (compaction),{" "}
              <strong>Z-ORDER</strong> (co-localisation des données) et{" "}
              <strong>VACUUM</strong> (nettoyage des fichiers obsolètes).
            </p>

            <h3 className="text-lg font-semibold text-[#1b3a4b]">
              📝 Instructions
            </h3>
            <ol className="list-decimal list-inside space-y-2 text-gray-700">
              <li>
                Créez une grande table <code>sales_raw</code> avec 1 million de
                lignes.
              </li>
              <li>
                Inspectez les détails de la table avec{" "}
                <code>DESCRIBE DETAIL</code>.
              </li>
              <li>
                Exécutez <code>OPTIMIZE</code> pour compacter les fichiers.
              </li>
              <li>
                Appliquez un <code>Z-ORDER</code> sur les colonnes fréquemment
                filtrées.
              </li>
              <li>
                Nettoyez les anciens fichiers avec <code>VACUUM</code>.
              </li>
              <li>Comparez les performances avant et après optimisation.</li>
            </ol>

            <InfoBox type="warning" title="Attention">
              <code>VACUUM</code> supprime définitivement les fichiers
              obsolètes. Après un VACUUM, le Time Travel ne pourra plus accéder
              aux versions antérieures dont les fichiers ont été supprimés. La
              rétention par défaut est de 168 heures (7 jours).
            </InfoBox>

            <SolutionToggle id="sol-2">
              <p className="text-sm text-gray-700 font-semibold mb-2">
                Étape 1 : Créer une grande table
              </p>
              <CodeBlock
                language="sql"
                title="Génération de données"
                code={`-- Créer une grande table non optimisée
CREATE OR REPLACE TABLE sales_raw AS
SELECT
  monotonically_increasing_id() AS id,
  CAST(rand() * 100 AS INT) AS store_id,
  CAST(rand() * 1000 AS INT) AS product_id,
  rand() * 500 AS amount,
  date_sub(current_date(), CAST(rand() * 365 AS INT)) AS sale_date
FROM range(1000000);`}
              />

              <p className="text-sm text-gray-700 font-semibold mb-2">
                Étape 2 : Inspecter les détails
              </p>
              <CodeBlock
                language="sql"
                title="Détails de la table"
                code={`-- Vérifier les fichiers (numFiles, sizeInBytes)
DESCRIBE DETAIL sales_raw;`}
              />
              <p className="text-sm text-gray-600">
                Observez le nombre de fichiers et la taille totale. Avant
                optimisation, vous constaterez probablement de nombreux petits
                fichiers.
              </p>

              <p className="text-sm text-gray-700 font-semibold mb-2">
                Étape 3 : Compaction et Z-ORDER
              </p>
              <CodeBlock
                language="sql"
                title="Optimisation"
                code={`-- Optimiser avec compaction (réduit le nombre de fichiers)
OPTIMIZE sales_raw;

-- Z-ORDER pour les requêtes fréquentes par store_id et sale_date
OPTIMIZE sales_raw ZORDER BY (store_id, sale_date);`}
              />
              <p className="text-sm text-gray-600">
                <strong>OPTIMIZE</strong> fusionne les petits fichiers en fichiers
                plus gros (par défaut ~1 Go). <strong>Z-ORDER</strong> réorganise
                les données pour co-localiser les valeurs fréquemment filtrées,
                ce qui accélère considérablement les requêtes avec filtres.
              </p>

              <p className="text-sm text-gray-700 font-semibold mb-2">
                Étape 4 : Nettoyage et test de performance
              </p>
              <CodeBlock
                language="sql"
                title="Vacuum et test"
                code={`-- Nettoyage des anciens fichiers (rétention de 168h minimum)
VACUUM sales_raw RETAIN 168 HOURS;

-- Performance : comparer avant/après
SELECT store_id, SUM(amount) FROM sales_raw
WHERE store_id = 42 AND sale_date > '2024-06-01'
GROUP BY store_id;`}
              />
              <p className="text-sm text-gray-600">
                Après le Z-ORDER, la requête filtrée sur{" "}
                <code>store_id</code> et <code>sale_date</code> devrait être
                significativement plus rapide car Spark peut sauter les fichiers
                qui ne contiennent pas les valeurs recherchées (data skipping).
              </p>
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
              Change Data Feed (CDF)
            </h2>
          </div>
          <div className="flex items-center gap-3 mb-5 ml-11">
            <span className="text-xs font-medium bg-gray-100 text-gray-600 px-2.5 py-1 rounded-full">
              ⏱ 45 min
            </span>
            <span className="text-xs font-medium bg-red-100 text-red-700 px-2.5 py-1 rounded-full">
              Avancé
            </span>
          </div>

          <div className="ml-11 space-y-4">
            <h3 className="text-lg font-semibold text-[#1b3a4b]">
              📖 Contexte
            </h3>
            <p className="text-gray-700 leading-relaxed">
              Le <strong>Change Data Feed</strong> (CDF) permet de capturer les
              modifications (insertions, mises à jour, suppressions) apportées
              à une table Delta. C&apos;est idéal pour alimenter des pipelines
              en aval qui n&apos;ont besoin que des changements incrémentaux
              plutôt que de retraiter l&apos;intégralité de la table.
            </p>

            <h3 className="text-lg font-semibold text-[#1b3a4b]">
              📝 Instructions
            </h3>
            <ol className="list-decimal list-inside space-y-2 text-gray-700">
              <li>
                Activez le CDF sur la table <code>customers</code>.
              </li>
              <li>
                Effectuez des modifications : INSERT, UPDATE et DELETE.
              </li>
              <li>
                Lisez les changements avec{" "}
                <code>table_changes()</code>.
              </li>
              <li>
                Filtrez les changements par type (insert, update_postimage,
                delete).
              </li>
            </ol>

            <InfoBox type="info" title="Colonnes CDF">
              Lorsque vous lisez les changements, Delta Lake ajoute
              automatiquement 3 colonnes :{" "}
              <code>_change_type</code> (insert, update_preimage,
              update_postimage, delete),{" "}
              <code>_commit_version</code> et{" "}
              <code>_commit_timestamp</code>.
            </InfoBox>

            <SolutionToggle id="sol-3">
              <p className="text-sm text-gray-700 font-semibold mb-2">
                Étape 1 : Activer le Change Data Feed
              </p>
              <CodeBlock
                language="sql"
                title="Activation du CDF"
                code={`-- Activer CDF sur une table existante
ALTER TABLE customers SET TBLPROPERTIES (delta.enableChangeDataFeed = true);`}
              />
              <p className="text-sm text-gray-600">
                Le CDF doit être activé explicitement. Il peut aussi être activé
                lors de la création :{" "}
                <code>
                  CREATE TABLE ... TBLPROPERTIES (delta.enableChangeDataFeed =
                  true)
                </code>
                .
              </p>

              <p className="text-sm text-gray-700 font-semibold mb-2">
                Étape 2 : Effectuer des modifications
              </p>
              <CodeBlock
                language="sql"
                title="Modifications de données"
                code={`-- Insertion
INSERT INTO customers VALUES (10, 'New Customer', 'new@email.com', 'Lyon', current_timestamp());

-- Mise à jour
UPDATE customers SET city = 'Lille' WHERE id = 1;

-- Suppression
DELETE FROM customers WHERE id = 3;`}
              />

              <p className="text-sm text-gray-700 font-semibold mb-2">
                Étape 3 : Lire les changements
              </p>
              <CodeBlock
                language="sql"
                title="Lecture du CDF"
                code={`-- Lire les changements à partir de la version 2
SELECT * FROM table_changes('customers', 2);
-- Colonnes supplémentaires : _change_type, _commit_version, _commit_timestamp

-- Filtrer par type de changement
SELECT * FROM table_changes('customers', 2)
WHERE _change_type IN ('update_postimage', 'insert');`}
              />
              <p className="text-sm text-gray-600">
                <strong>update_preimage</strong> contient la ligne{" "}
                <em>avant</em> la mise à jour,{" "}
                <strong>update_postimage</strong> contient la ligne{" "}
                <em>après</em>. Pour un pipeline en aval, on utilise
                généralement <code>insert</code> et{" "}
                <code>update_postimage</code> pour obtenir l&apos;état le plus
                récent.
              </p>
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
              Schéma Evolution &amp; Enforcement
            </h2>
          </div>
          <div className="flex items-center gap-3 mb-5 ml-11">
            <span className="text-xs font-medium bg-gray-100 text-gray-600 px-2.5 py-1 rounded-full">
              ⏱ 45 min
            </span>
            <span className="text-xs font-medium bg-red-100 text-red-700 px-2.5 py-1 rounded-full">
              Avancé
            </span>
          </div>

          <div className="ml-11 space-y-4">
            <h3 className="text-lg font-semibold text-[#1b3a4b]">
              📖 Contexte
            </h3>
            <p className="text-gray-700 leading-relaxed">
              Delta Lake impose par défaut un <strong>schema enforcement</strong>{" "}
              : toute écriture dont le schéma ne correspond pas à celui de la
              table sera rejetée. C&apos;est une protection essentielle pour la
              qualité des données. Cependant, il est possible d&apos;activer le{" "}
              <strong>schema evolution</strong> pour accepter de nouvelles
              colonnes à la volée avec l&apos;option{" "}
              <code>mergeSchema</code>.
            </p>

            <h3 className="text-lg font-semibold text-[#1b3a4b]">
              📝 Instructions
            </h3>
            <ol className="list-decimal list-inside space-y-2 text-gray-700">
              <li>
                Créez une table <code>schema_test</code> avec un schéma initial
                (id, name, value).
              </li>
              <li>
                Essayez d&apos;écrire des données avec une colonne
                supplémentaire : constatez l&apos;échec (schema enforcement).
              </li>
              <li>
                Activez le schema evolution avec l&apos;option{" "}
                <code>mergeSchema</code> et réessayez.
              </li>
              <li>Vérifiez que la table contient bien la nouvelle colonne.</li>
            </ol>

            <InfoBox type="important" title="Point certification">
              La différence entre <strong>schema enforcement</strong> et{" "}
              <strong>schema evolution</strong> est un sujet fréquent à
              l&apos;examen. Retenez : enforcement = protection par défaut,
              evolution = opt-in avec <code>mergeSchema</code>.
            </InfoBox>

            <SolutionToggle id="sol-4">
              <p className="text-sm text-gray-700 font-semibold mb-2">
                Étape 1 : Créer la table avec un schéma v1
              </p>
              <CodeBlock
                language="python"
                title="Création avec schéma initial"
                code={`from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Créer une table avec un schéma strict
schema_v1 = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("value", DoubleType())
])

df1 = spark.createDataFrame([(1, "A", 10.0), (2, "B", 20.0)], schema_v1)
df1.write.format("delta").mode("overwrite").saveAsTable("schema_test")`}
              />

              <p className="text-sm text-gray-700 font-semibold mb-2">
                Étape 2 : Échec du schema enforcement
              </p>
              <CodeBlock
                language="python"
                title="Schema enforcement (échec)"
                code={`# Schéma v2 avec une colonne supplémentaire
schema_v2 = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("value", DoubleType()),
    StructField("category", StringType())  # Nouvelle colonne!
])

df2 = spark.createDataFrame([(3, "C", 30.0, "X")], schema_v2)

# Ceci ÉCHOUE : Schema enforcement rejectera l'écriture
# car la colonne "category" n'existe pas dans le schéma de la table
# df2.write.format("delta").mode("append").saveAsTable("schema_test")`}
              />
              <p className="text-sm text-gray-600">
                Si vous décommentez la dernière ligne, Spark lèvera une
                exception <code>AnalysisException</code> car le DataFrame
                contient une colonne (<code>category</code>) absente du schéma
                de la table cible.
              </p>

              <p className="text-sm text-gray-700 font-semibold mb-2">
                Étape 3 : Schema evolution avec mergeSchema
              </p>
              <CodeBlock
                language="python"
                title="Schema evolution (succès)"
                code={`# Ceci FONCTIONNE : Schema evolution activée
df2.write.format("delta") \\
    .mode("append") \\
    .option("mergeSchema", "true") \\
    .saveAsTable("schema_test")

# Vérifier le résultat
display(spark.table("schema_test"))`}
              />
              <p className="text-sm text-gray-600">
                Avec <code>mergeSchema = true</code>, Delta Lake accepte la
                nouvelle colonne et met à jour le schéma de la table. Les lignes
                existantes auront <code>null</code> pour la colonne{" "}
                <code>category</code>.
              </p>

              <InfoBox type="tip" title="Bonne pratique">
                N&apos;activez <code>mergeSchema</code> que de manière
                intentionnelle. En production, privilégiez le schema enforcement
                par défaut et gérez les évolutions de schéma via des migrations
                contrôlées (ALTER TABLE ADD COLUMN).
              </InfoBox>
            </SolutionToggle>
          </div>
        </section>

        {/* Navigation bas de page */}
        <div className="mt-16 pt-8 border-t border-gray-200 flex flex-col sm:flex-row items-center justify-between gap-4">
          <Link
            href="/exercices"
            className="inline-flex items-center gap-2 text-sm text-[#1b3a4b] hover:text-[#ff3621] font-medium transition-colors"
          >
            ← Retour aux exercices
          </Link>
          <Link
            href="/exercices/quiz-certification"
            className="inline-flex items-center gap-2 px-5 py-2.5 rounded-lg bg-[#ff3621] text-white text-sm font-semibold hover:bg-[#e0301d] transition-colors"
          >
            Quiz de certification →
          </Link>
        </div>
      </div>
    </div>
  );
}
