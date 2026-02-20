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

export default function FondamentauxExercicesPage() {
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
            <span className="inline-flex items-center px-3 py-1 rounded-full text-xs font-semibold bg-green-400/20 text-green-200 border border-green-400/30">
              Débutant
            </span>
            <span className="text-sm text-white/70">⏱ 3 heures</span>
            <span className="text-sm text-white/70">
              📘 Modules 1 &amp; 2
            </span>
          </div>
          <h1 className="text-3xl lg:text-4xl font-extrabold mb-3">
            🧱 Exercices : Fondamentaux Databricks
          </h1>
          <p className="text-lg text-white/80 max-w-2xl leading-relaxed">
            5 exercices progressifs pour maîtriser les bases : clusters,
            notebooks, bases de données, tables, vues, MERGE et fonctions SQL
            avancées.
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
                Configuration de l&apos;environnement{" "}
                <span className="text-gray-400">(30 min)</span>
              </span>
            </li>
            <li className="flex items-center gap-2">
              <span className="w-6 h-6 flex items-center justify-center bg-[#1b3a4b] text-white text-xs font-bold rounded-full">
                2
              </span>
              <span>
                Création de base de données et tables{" "}
                <span className="text-gray-400">(45 min)</span>
              </span>
            </li>
            <li className="flex items-center gap-2">
              <span className="w-6 h-6 flex items-center justify-center bg-[#1b3a4b] text-white text-xs font-bold rounded-full">
                3
              </span>
              <span>
                Vues et CTEs <span className="text-gray-400">(30 min)</span>
              </span>
            </li>
            <li className="flex items-center gap-2">
              <span className="w-6 h-6 flex items-center justify-center bg-[#1b3a4b] text-white text-xs font-bold rounded-full">
                4
              </span>
              <span>
                Transformations avec MERGE{" "}
                <span className="text-gray-400">(45 min)</span>
              </span>
            </li>
            <li className="flex items-center gap-2">
              <span className="w-6 h-6 flex items-center justify-center bg-[#1b3a4b] text-white text-xs font-bold rounded-full">
                5
              </span>
              <span>
                Fonctions avancées{" "}
                <span className="text-gray-400">(30 min)</span>
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
              Configuration de l&apos;environnement
            </h2>
          </div>
          <div className="flex items-center gap-3 mb-5 ml-11">
            <span className="text-xs font-medium bg-gray-100 text-gray-600 px-2.5 py-1 rounded-full">
              ⏱ 30 min
            </span>
            <span className="text-xs font-medium bg-green-100 text-green-700 px-2.5 py-1 rounded-full">
              Débutant
            </span>
          </div>

          {/* Contexte */}
          <div className="ml-11 space-y-4">
            <h3 className="text-lg font-semibold text-[#1b3a4b]">
              📖 Contexte
            </h3>
            <p className="text-gray-700 leading-relaxed">
              Avant de commencer à travailler avec Databricks, vous devez
              configurer votre environnement de travail. Cela inclut la
              création d&apos;un cluster de calcul et d&apos;un notebook pour
              exécuter vos commandes.
            </p>

            <h3 className="text-lg font-semibold text-[#1b3a4b]">
              📝 Instructions
            </h3>
            <ol className="list-decimal list-inside space-y-2 text-gray-700">
              <li>
                Créez un cluster avec les caractéristiques suivantes :
                <ul className="list-disc list-inside ml-5 mt-1 space-y-1 text-sm text-gray-600">
                  <li>
                    Mode : <strong>Single Node</strong>
                  </li>
                  <li>
                    Runtime : <strong>13.3 LTS</strong> (ou la dernière LTS
                    disponible)
                  </li>
                  <li>
                    Taille : <strong>4 cores</strong>
                  </li>
                  <li>
                    Auto-termination : <strong>30 minutes</strong>
                  </li>
                </ul>
              </li>
              <li>Créez un notebook Python et attachez-le à votre cluster.</li>
              <li>
                Exécutez le code suivant pour vérifier la connexion :
              </li>
            </ol>

            <CodeBlock
              language="python"
              title="Vérification de la connexion"
              code={`print(f"Spark version: {spark.version}")
print(f"Cluster: {spark.conf.get('spark.databricks.clusterUsageTags.clusterName')}")

# Vérifier que le contexte Spark fonctionne
df = spark.range(10)
df.show()
print(f"Nombre de lignes : {df.count()}")`}
            />

            <h3 className="text-lg font-semibold text-[#1b3a4b]">
              ✅ Résultat attendu
            </h3>
            <ul className="list-disc list-inside text-sm text-gray-700 space-y-1">
              <li>Le cluster démarre correctement (état &quot;Running&quot;)</li>
              <li>La version de Spark s&apos;affiche (ex: 3.4.1)</li>
              <li>Le nom du cluster s&apos;affiche</li>
              <li>Le DataFrame de 0 à 9 s&apos;affiche</li>
            </ul>

            <InfoBox type="tip" title="Astuce">
              <p>
                Utilisez toujours un runtime <strong>LTS</strong> (Long Term
                Support) pour la stabilité. Activez l&apos;auto-termination
                pour éviter les coûts inutiles.
              </p>
            </InfoBox>

            <SolutionToggle id="sol-1">
              <p className="text-sm text-gray-700">
                <strong>Étape par étape :</strong>
              </p>
              <ol className="list-decimal list-inside text-sm text-gray-700 space-y-2">
                <li>
                  Dans la barre latérale, cliquez sur{" "}
                  <strong>Compute</strong> → <strong>Create Cluster</strong>.
                </li>
                <li>
                  Nommez le cluster (ex: &quot;cluster-exercices&quot;).
                </li>
                <li>
                  Sélectionnez <strong>Single Node</strong> dans la section
                  Cluster Mode.
                </li>
                <li>
                  Choisissez le runtime <strong>13.3 LTS</strong>.
                </li>
                <li>
                  Dans <strong>Advanced Options</strong>, réglez
                  l&apos;auto-termination à 30 minutes.
                </li>
                <li>
                  Cliquez sur <strong>Create Cluster</strong> et attendez le
                  démarrage (2-5 min).
                </li>
                <li>
                  Créez un nouveau notebook : <strong>Workspace</strong> →{" "}
                  <strong>Create</strong> → <strong>Notebook</strong>.
                </li>
                <li>
                  Sélectionnez <strong>Python</strong> comme langage par
                  défaut.
                </li>
                <li>
                  Attachez le notebook au cluster créé, puis exécutez le code.
                </li>
              </ol>
              <CodeBlock
                language="python"
                title="Code complet de vérification"
                code={`# Vérification complète de l'environnement
print("=" * 50)
print("VÉRIFICATION DE L'ENVIRONNEMENT")
print("=" * 50)
print(f"Spark version: {spark.version}")
print(f"Cluster: {spark.conf.get('spark.databricks.clusterUsageTags.clusterName')}")
print(f"Nombre de cores: {spark.sparkContext.defaultParallelism}")

# Test simple
df = spark.range(10)
df.show()
print(f"✅ Tout fonctionne ! {df.count()} lignes générées.")`}
              />
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
              Création de base de données et tables
            </h2>
          </div>
          <div className="flex items-center gap-3 mb-5 ml-11">
            <span className="text-xs font-medium bg-gray-100 text-gray-600 px-2.5 py-1 rounded-full">
              ⏱ 45 min
            </span>
            <span className="text-xs font-medium bg-green-100 text-green-700 px-2.5 py-1 rounded-full">
              Débutant
            </span>
          </div>

          <div className="ml-11 space-y-4">
            <h3 className="text-lg font-semibold text-[#1b3a4b]">
              📖 Contexte
            </h3>
            <p className="text-gray-700 leading-relaxed">
              Vous travaillez pour une entreprise e-commerce. Votre mission est
              de créer la base de données et les tables nécessaires pour stocker
              les informations des clients et des commandes.
            </p>

            <h3 className="text-lg font-semibold text-[#1b3a4b]">
              📝 Instructions
            </h3>
            <ol className="list-decimal list-inside space-y-2 text-gray-700">
              <li>
                Créez une base de données <code className="bg-gray-100 px-1.5 py-0.5 rounded text-sm font-mono">ecommerce_db</code>.
              </li>
              <li>
                Créez une table managée <code className="bg-gray-100 px-1.5 py-0.5 rounded text-sm font-mono">customers</code> avec
                les colonnes : <strong>id</strong> (INT),{" "}
                <strong>name</strong> (STRING), <strong>email</strong>{" "}
                (STRING), <strong>city</strong> (STRING),{" "}
                <strong>created_at</strong> (TIMESTAMP).
              </li>
              <li>Insérez 5 lignes de données exemple.</li>
              <li>
                Créez une table externe <code className="bg-gray-100 px-1.5 py-0.5 rounded text-sm font-mono">orders</code> avec les
                colonnes : <strong>order_id</strong> (INT),{" "}
                <strong>customer_id</strong> (INT),{" "}
                <strong>product</strong> (STRING),{" "}
                <strong>amount</strong> (DOUBLE),{" "}
                <strong>order_date</strong> (DATE).
              </li>
              <li>
                Vérifiez les métadonnées avec{" "}
                <code className="bg-gray-100 px-1.5 py-0.5 rounded text-sm font-mono">DESCRIBE EXTENDED</code>.
              </li>
            </ol>

            <CodeBlock
              language="sql"
              title="Code à écrire"
              code={`-- Étape 1 : Créer la base de données
CREATE DATABASE IF NOT EXISTS ecommerce_db;
USE ecommerce_db;

-- Étape 2 : Créer la table customers (managée)
CREATE TABLE customers (
  id INT,
  name STRING,
  email STRING,
  city STRING,
  created_at TIMESTAMP
);

-- Étape 3 : Insérer des données
INSERT INTO customers VALUES
  (1, 'Marie Dupont', 'marie@email.com', 'Paris', current_timestamp()),
  (2, 'Jean Martin', 'jean@email.com', 'Lyon', current_timestamp()),
  (3, 'Sophie Bernard', 'sophie@email.com', 'Marseille', current_timestamp()),
  (4, 'Pierre Durand', 'pierre@email.com', 'Toulouse', current_timestamp()),
  (5, 'Claire Moreau', 'claire@email.com', 'Bordeaux', current_timestamp());

-- Étape 4 : Vérifier les données
SELECT * FROM customers;

-- Étape 5 : Examiner les métadonnées
DESCRIBE EXTENDED customers;`}
            />

            <h3 className="text-lg font-semibold text-[#1b3a4b]">
              ✅ Résultat attendu
            </h3>
            <ul className="list-disc list-inside text-sm text-gray-700 space-y-1">
              <li>La base de données est créée avec succès</li>
              <li>La table contient 5 lignes de données clients</li>
              <li>
                DESCRIBE EXTENDED montre le type &quot;MANAGED&quot; et
                l&apos;emplacement par défaut
              </li>
            </ul>

            <InfoBox type="info" title="Table managée vs externe">
              <p>
                Une <strong>table managée</strong> stocke les données et les
                métadonnées dans le metastore. Si vous la supprimez, les
                données sont aussi supprimées. Une <strong>table externe</strong>{" "}
                ne gère que les métadonnées — les données restent à leur
                emplacement d&apos;origine.
              </p>
            </InfoBox>

            <InfoBox type="warning" title="Erreur courante">
              <p>
                N&apos;oubliez pas le <code className="bg-amber-100 px-1 rounded text-sm font-mono">USE ecommerce_db</code> avant de
                créer les tables, sinon elles seront créées dans la base{" "}
                <code className="bg-amber-100 px-1 rounded text-sm font-mono">default</code>.
              </p>
            </InfoBox>

            <SolutionToggle id="sol-2">
              <p className="text-sm text-gray-700 mb-2">
                <strong>Solution complète avec la table orders :</strong>
              </p>
              <CodeBlock
                language="sql"
                title="Solution - Base de données et tables"
                code={`-- Créer et utiliser la base de données
CREATE DATABASE IF NOT EXISTS ecommerce_db;
USE ecommerce_db;

-- Table managée customers
CREATE TABLE IF NOT EXISTS customers (
  id INT,
  name STRING,
  email STRING,
  city STRING,
  created_at TIMESTAMP
);

INSERT INTO customers VALUES
  (1, 'Marie Dupont', 'marie@email.com', 'Paris', current_timestamp()),
  (2, 'Jean Martin', 'jean@email.com', 'Lyon', current_timestamp()),
  (3, 'Sophie Bernard', 'sophie@email.com', 'Marseille', current_timestamp()),
  (4, 'Pierre Durand', 'pierre@email.com', 'Toulouse', current_timestamp()),
  (5, 'Claire Moreau', 'claire@email.com', 'Bordeaux', current_timestamp());

-- Table externe orders
CREATE TABLE IF NOT EXISTS orders (
  order_id INT,
  customer_id INT,
  product STRING,
  amount DOUBLE,
  order_date DATE
)
LOCATION '/mnt/data/orders';

INSERT INTO orders VALUES
  (101, 1, 'Laptop', 999.99, '2025-02-01'),
  (102, 1, 'Souris', 29.99, '2025-02-03'),
  (103, 2, 'Clavier', 79.99, '2025-02-05'),
  (104, 3, 'Écran', 349.99, '2025-02-10'),
  (105, 4, 'Casque', 149.99, '2025-02-12');

-- Vérifications
SELECT * FROM customers ORDER BY id;
SELECT * FROM orders ORDER BY order_id;

-- Métadonnées
DESCRIBE EXTENDED customers;
DESCRIBE EXTENDED orders;`}
              />
              <p className="text-sm text-gray-600 mt-2">
                Notez la différence dans la sortie de DESCRIBE EXTENDED :{" "}
                <strong>customers</strong> affiche &quot;Type: MANAGED&quot; tandis
                qu&apos;<strong>orders</strong> affiche &quot;Type: EXTERNAL&quot; avec
                le chemin LOCATION spécifié.
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
              Vues et CTEs
            </h2>
          </div>
          <div className="flex items-center gap-3 mb-5 ml-11">
            <span className="text-xs font-medium bg-gray-100 text-gray-600 px-2.5 py-1 rounded-full">
              ⏱ 30 min
            </span>
            <span className="text-xs font-medium bg-green-100 text-green-700 px-2.5 py-1 rounded-full">
              Débutant
            </span>
          </div>

          <div className="ml-11 space-y-4">
            <h3 className="text-lg font-semibold text-[#1b3a4b]">
              📖 Contexte
            </h3>
            <p className="text-gray-700 leading-relaxed">
              L&apos;équipe marketing veut accéder facilement aux clients
              parisiens et aux commandes récentes. Vous devez créer des vues
              pour simplifier ces requêtes fréquentes.
            </p>

            <h3 className="text-lg font-semibold text-[#1b3a4b]">
              📝 Instructions
            </h3>
            <ol className="list-decimal list-inside space-y-2 text-gray-700">
              <li>
                Créez une vue permanente{" "}
                <code className="bg-gray-100 px-1.5 py-0.5 rounded text-sm font-mono">paris_customers</code>{" "}
                filtrant les clients de Paris.
              </li>
              <li>
                Créez une vue temporaire{" "}
                <code className="bg-gray-100 px-1.5 py-0.5 rounded text-sm font-mono">recent_orders</code>{" "}
                pour les commandes des 30 derniers jours.
              </li>
              <li>
                Écrivez une requête CTE qui joint clients et commandes pour
                obtenir le montant total par client.
              </li>
            </ol>

            <CodeBlock
              language="sql"
              title="Code à écrire"
              code={`-- Vue permanente : clients parisiens
CREATE VIEW paris_customers AS
SELECT * FROM customers WHERE city = 'Paris';

-- Vue temporaire : commandes récentes
CREATE OR REPLACE TEMP VIEW recent_orders AS
SELECT * FROM orders WHERE order_date >= date_sub(current_date(), 30);

-- CTE : montant total par client
WITH customer_totals AS (
  SELECT c.name, SUM(o.amount) as total_spent
  FROM customers c
  JOIN orders o ON c.id = o.customer_id
  GROUP BY c.name
)
SELECT * FROM customer_totals ORDER BY total_spent DESC;`}
            />

            <h3 className="text-lg font-semibold text-[#1b3a4b]">
              ✅ Résultat attendu
            </h3>
            <ul className="list-disc list-inside text-sm text-gray-700 space-y-1">
              <li>
                La vue <code className="bg-gray-100 px-1 rounded text-sm font-mono">paris_customers</code> retourne uniquement
                les clients de Paris
              </li>
              <li>
                La vue temporaire <code className="bg-gray-100 px-1 rounded text-sm font-mono">recent_orders</code> filtre
                correctement par date
              </li>
              <li>
                La CTE affiche le total dépensé par client, trié du plus
                grand au plus petit
              </li>
            </ul>

            <InfoBox type="tip" title="Différences entre les vues">
              <p>
                <strong>Vue permanente :</strong> persiste dans le metastore,
                accessible par tous les notebooks.
                <br />
                <strong>Vue temporaire :</strong> existe uniquement pour la
                session Spark active.
                <br />
                <strong>Vue temporaire globale :</strong> partagée entre
                notebooks du même cluster (accessible via{" "}
                <code className="bg-emerald-100 px-1 rounded text-sm font-mono">global_temp.nom_vue</code>).
              </p>
            </InfoBox>

            <SolutionToggle id="sol-3">
              <CodeBlock
                language="sql"
                title="Solution complète - Vues et CTEs"
                code={`USE ecommerce_db;

-- 1. Vue permanente
CREATE OR REPLACE VIEW paris_customers AS
SELECT * FROM customers WHERE city = 'Paris';

-- Vérification
SELECT * FROM paris_customers;
-- Résultat : Marie Dupont (Paris)

-- 2. Vue temporaire
CREATE OR REPLACE TEMP VIEW recent_orders AS
SELECT * FROM orders WHERE order_date >= date_sub(current_date(), 30);

-- Vérification
SELECT * FROM recent_orders;

-- 3. CTE avec jointure et agrégation
WITH customer_totals AS (
  SELECT 
    c.name,
    c.city,
    COUNT(o.order_id) AS nb_commandes,
    SUM(o.amount) AS total_spent,
    AVG(o.amount) AS panier_moyen
  FROM customers c
  JOIN orders o ON c.id = o.customer_id
  GROUP BY c.name, c.city
)
SELECT 
  name,
  city,
  nb_commandes,
  ROUND(total_spent, 2) AS total_spent,
  ROUND(panier_moyen, 2) AS panier_moyen
FROM customer_totals 
ORDER BY total_spent DESC;

-- Bonus : vérifier les vues existantes
SHOW VIEWS IN ecommerce_db;`}
              />
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
              Transformations avec MERGE
            </h2>
          </div>
          <div className="flex items-center gap-3 mb-5 ml-11">
            <span className="text-xs font-medium bg-gray-100 text-gray-600 px-2.5 py-1 rounded-full">
              ⏱ 45 min
            </span>
            <span className="text-xs font-medium bg-amber-100 text-amber-700 px-2.5 py-1 rounded-full">
              Intermédiaire
            </span>
          </div>

          <div className="ml-11 space-y-4">
            <h3 className="text-lg font-semibold text-[#1b3a4b]">
              📖 Contexte
            </h3>
            <p className="text-gray-700 leading-relaxed">
              L&apos;équipe CRM vous envoie régulièrement des mises à jour de
              clients. Certains sont des clients existants avec des
              informations modifiées, d&apos;autres sont de nouveaux clients.
              Vous devez mettre en place un mécanisme d&apos;upsert avec{" "}
              <strong>MERGE INTO</strong>.
            </p>

            <h3 className="text-lg font-semibold text-[#1b3a4b]">
              📝 Instructions
            </h3>
            <ol className="list-decimal list-inside space-y-2 text-gray-700">
              <li>
                Créez une vue temporaire{" "}
                <code className="bg-gray-100 px-1.5 py-0.5 rounded text-sm font-mono">customers_update</code>{" "}
                contenant des données de mise à jour (un client existant
                modifié + un nouveau client).
              </li>
              <li>
                Utilisez <strong>MERGE INTO</strong> pour effectuer
                l&apos;upsert.
              </li>
              <li>Vérifiez que le résultat est correct.</li>
            </ol>

            <CodeBlock
              language="sql"
              title="Code à écrire"
              code={`-- Créer une table staging avec des modifications
CREATE OR REPLACE TEMP VIEW customers_update AS
SELECT * FROM VALUES
  (1, 'Marie Dupont', 'marie.new@email.com', 'Paris', current_timestamp()),
  (6, 'Luc Petit', 'luc@email.com', 'Nice', current_timestamp())
AS t(id, name, email, city, created_at);

-- MERGE pour upsert
MERGE INTO customers c
USING customers_update u
ON c.id = u.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- Vérifier le résultat
SELECT * FROM customers ORDER BY id;`}
            />

            <h3 className="text-lg font-semibold text-[#1b3a4b]">
              ✅ Résultat attendu
            </h3>
            <ul className="list-disc list-inside text-sm text-gray-700 space-y-1">
              <li>
                Le client id=1 (Marie Dupont) a son email mis à jour vers
                &quot;marie.new@email.com&quot;
              </li>
              <li>
                Un nouveau client id=6 (Luc Petit) est ajouté
              </li>
              <li>La table contient maintenant 6 lignes</li>
            </ul>

            <InfoBox type="warning" title="Erreurs courantes avec MERGE">
              <p>
                <strong>1.</strong> La clé de jointure (ON) doit être unique
                côté source. Si plusieurs lignes source correspondent à une
                même ligne cible, vous obtiendrez une erreur.
                <br />
                <strong>2.</strong> N&apos;oubliez pas que{" "}
                <code className="bg-amber-100 px-1 rounded text-sm font-mono">UPDATE SET *</code>{" "}
                met à jour toutes les colonnes — assurez-vous que les schémas
                correspondent.
              </p>
            </InfoBox>

            <SolutionToggle id="sol-4">
              <CodeBlock
                language="sql"
                title="Solution complète - MERGE INTO"
                code={`USE ecommerce_db;

-- Données de mise à jour
CREATE OR REPLACE TEMP VIEW customers_update AS
SELECT * FROM VALUES
  (1, 'Marie Dupont', 'marie.new@email.com', 'Paris', current_timestamp()),
  (6, 'Luc Petit', 'luc@email.com', 'Nice', current_timestamp())
AS t(id, name, email, city, created_at);

-- Voir les données avant le MERGE
SELECT 'AVANT' AS status, * FROM customers ORDER BY id;

-- MERGE INTO (upsert)
MERGE INTO customers c
USING customers_update u
ON c.id = u.id
WHEN MATCHED THEN 
  UPDATE SET 
    c.name = u.name,
    c.email = u.email,
    c.city = u.city,
    c.created_at = u.created_at
WHEN NOT MATCHED THEN 
  INSERT (id, name, email, city, created_at)
  VALUES (u.id, u.name, u.email, u.city, u.created_at);

-- Voir les données après le MERGE
SELECT 'APRÈS' AS status, * FROM customers ORDER BY id;

-- Vérification spécifique
SELECT * FROM customers WHERE id IN (1, 6);
-- id=1 : email mis à jour → marie.new@email.com
-- id=6 : nouveau client → Luc Petit`}
              />
              <p className="text-sm text-gray-600 mt-2">
                💡 Vous pouvez aussi utiliser{" "}
                <code className="bg-gray-100 px-1 rounded text-sm font-mono">WHEN MATCHED AND condition THEN</code>{" "}
                pour ajouter des conditions supplémentaires au MERGE.
              </p>
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
              Fonctions avancées
            </h2>
          </div>
          <div className="flex items-center gap-3 mb-5 ml-11">
            <span className="text-xs font-medium bg-gray-100 text-gray-600 px-2.5 py-1 rounded-full">
              ⏱ 30 min
            </span>
            <span className="text-xs font-medium bg-amber-100 text-amber-700 px-2.5 py-1 rounded-full">
              Intermédiaire
            </span>
          </div>

          <div className="ml-11 space-y-4">
            <h3 className="text-lg font-semibold text-[#1b3a4b]">
              📖 Contexte
            </h3>
            <p className="text-gray-700 leading-relaxed">
              Vous devez manipuler des données complexes contenant des
              tableaux (arrays) et créer des fonctions réutilisables pour les
              calculs métier de votre entreprise e-commerce.
            </p>

            <h3 className="text-lg font-semibold text-[#1b3a4b]">
              📝 Instructions
            </h3>
            <ol className="list-decimal list-inside space-y-2 text-gray-700">
              <li>
                Créez une vue temporaire{" "}
                <code className="bg-gray-100 px-1.5 py-0.5 rounded text-sm font-mono">products</code>{" "}
                avec une colonne de type ARRAY contenant des prix.
              </li>
              <li>
                Utilisez <strong>FILTER</strong> pour extraire les prix
                supérieurs à 600€.
              </li>
              <li>
                Utilisez <strong>TRANSFORM</strong> pour appliquer une
                réduction de 10% sur tous les prix.
              </li>
              <li>
                Créez un <strong>UDF SQL</strong> pour calculer le prix TTC
                (TVA 20%).
              </li>
            </ol>

            <CodeBlock
              language="sql"
              title="Code à écrire"
              code={`-- Table avec arrays
CREATE OR REPLACE TEMP VIEW products AS
SELECT * FROM VALUES
  (1, 'Laptop', ARRAY(999.99, 1099.99, 899.99)),
  (2, 'Phone', ARRAY(599.99, 499.99, 699.99))
AS t(id, name, prices);

-- FILTER : prix > 600
SELECT name, FILTER(prices, p -> p > 600) AS expensive_prices 
FROM products;

-- TRANSFORM : appliquer une réduction de 10%
SELECT name, TRANSFORM(prices, p -> p * 0.9) AS discounted 
FROM products;

-- UDF pour calculer le TTC
CREATE OR REPLACE FUNCTION calculate_ttc(prix DOUBLE)
RETURNS DOUBLE
RETURN prix * 1.20;

SELECT calculate_ttc(100.0);`}
            />

            <h3 className="text-lg font-semibold text-[#1b3a4b]">
              ✅ Résultat attendu
            </h3>
            <ul className="list-disc list-inside text-sm text-gray-700 space-y-1">
              <li>
                FILTER retourne : Laptop → [999.99, 1099.99],
                Phone → [699.99]
              </li>
              <li>
                TRANSFORM retourne les prix avec 10% de réduction
              </li>
              <li>
                <code className="bg-gray-100 px-1 rounded text-sm font-mono">calculate_ttc(100.0)</code>{" "}
                retourne <strong>120.0</strong>
              </li>
            </ul>

            <InfoBox type="tip" title="Fonctions d'ordre supérieur">
              <p>
                <strong>FILTER</strong>, <strong>TRANSFORM</strong> et{" "}
                <strong>EXISTS</strong> sont des fonctions d&apos;ordre
                supérieur qui prennent une fonction lambda en paramètre. La
                syntaxe est :{" "}
                <code className="bg-emerald-100 px-1 rounded text-sm font-mono">
                  FONCTION(array, element -&gt; expression)
                </code>
              </p>
            </InfoBox>

            <InfoBox type="info" title="UDF SQL vs Python">
              <p>
                Les UDF SQL sont plus performantes que les UDF Python car
                elles sont exécutées directement par le moteur Spark sans
                sérialisation. Préférez toujours les UDF SQL quand c&apos;est
                possible.
              </p>
            </InfoBox>

            <SolutionToggle id="sol-5">
              <CodeBlock
                language="sql"
                title="Solution complète - Fonctions avancées"
                code={`-- 1. Créer la vue avec des arrays
CREATE OR REPLACE TEMP VIEW products AS
SELECT * FROM VALUES
  (1, 'Laptop', ARRAY(999.99, 1099.99, 899.99)),
  (2, 'Phone', ARRAY(599.99, 499.99, 699.99)),
  (3, 'Tablet', ARRAY(329.99, 449.99, 299.99))
AS t(id, name, prices);

-- 2. FILTER : ne garder que les prix > 600
SELECT 
  name, 
  prices AS all_prices,
  FILTER(prices, p -> p > 600) AS expensive_prices 
FROM products;
-- Laptop: [999.99, 1099.99]
-- Phone: [699.99]
-- Tablet: []

-- 3. TRANSFORM : réduction de 10%
SELECT 
  name,
  prices AS original_prices,
  TRANSFORM(prices, p -> ROUND(p * 0.9, 2)) AS discounted_prices
FROM products;

-- 4. EXISTS : vérifier si au moins un prix > 1000
SELECT 
  name,
  EXISTS(prices, p -> p > 1000) AS has_premium_price
FROM products;
-- Laptop: true, Phone: false, Tablet: false

-- 5. Combiner FILTER + TRANSFORM
SELECT 
  name,
  TRANSFORM(
    FILTER(prices, p -> p > 500),
    p -> ROUND(p * 0.9, 2)
  ) AS discounted_expensive
FROM products;

-- 6. UDF SQL
CREATE OR REPLACE FUNCTION calculate_ttc(prix DOUBLE)
RETURNS DOUBLE
RETURN prix * 1.20;

-- Utiliser l'UDF
SELECT 
  name,
  TRANSFORM(prices, p -> ROUND(calculate_ttc(p), 2)) AS prices_ttc
FROM products;

-- 7. UDF plus complexe
CREATE OR REPLACE FUNCTION format_price(prix DOUBLE, devise STRING)
RETURNS STRING
RETURN CONCAT(ROUND(prix, 2), ' ', devise);

SELECT format_price(99.999, '€');
-- Résultat : "100.0 €"`}
              />
            </SolutionToggle>
          </div>
        </section>

        {/* Récapitulatif */}
        <div className="bg-gradient-to-r from-[#1b3a4b]/5 to-[#ff3621]/5 rounded-xl border border-gray-200 p-6 mb-10">
          <h3 className="text-lg font-bold text-[#1b3a4b] mb-3">
            🎓 Récapitulatif
          </h3>
          <p className="text-sm text-gray-700 mb-3">
            En complétant ces 5 exercices, vous avez pratiqué :
          </p>
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-2">
            {[
              "✅ Configuration d'un cluster Databricks",
              "✅ Création de bases de données et tables",
              "✅ Tables managées vs externes",
              "✅ Vues permanentes et temporaires",
              "✅ CTEs et requêtes complexes",
              "✅ MERGE INTO pour les upserts",
              "✅ Fonctions d'ordre supérieur (FILTER, TRANSFORM)",
              "✅ Création d'UDF SQL",
            ].map((item) => (
              <span key={item} className="text-sm text-gray-700">
                {item}
              </span>
            ))}
          </div>
        </div>

        {/* Navigation */}
        <div className="flex flex-wrap justify-between gap-4 pt-6 border-t border-gray-200">
          <Link
            href="/exercices"
            className="inline-flex items-center gap-2 px-5 py-2.5 rounded-lg text-sm font-semibold bg-gray-100 text-[#1b3a4b] hover:bg-gray-200 transition-colors"
          >
            ← Tous les exercices
          </Link>
          <Link
            href="/exercices/streaming-multi-hop"
            className="inline-flex items-center gap-2 px-5 py-2.5 rounded-lg text-sm font-semibold bg-[#1b3a4b] text-white hover:bg-[#2d5f7a] transition-colors"
          >
            Exercice suivant : Streaming & Multi-Hop →
          </Link>
        </div>
      </div>
    </div>
  );
}
