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
    question: "Quelle commande permet de faire un upsert (INSERT ou UPDATE) ?",
    options: [
      "INSERT OR UPDATE",
      "UPSERT INTO",
      "MERGE INTO",
      "UPDATE OR INSERT"
    ],
    correctIndex: 2,
    explanation: "MERGE INTO est la commande SQL standard pour effectuer un upsert dans Databricks. Elle permet de combiner INSERT, UPDATE et DELETE en une seule opération basée sur une condition de correspondance."
  },
  {
    question: "MERGE INTO est-il idempotent ?",
    options: [
      "Non, il crée des doublons à chaque exécution",
      "Oui, exécuter MERGE plusieurs fois produit le même résultat",
      "Seulement si on utilise la clause WHEN NOT MATCHED",
      "Non, il échoue à la deuxième exécution"
    ],
    correctIndex: 1,
    explanation: "Oui, MERGE INTO est idempotent. La condition ON détermine la correspondance, donc exécuter MERGE plusieurs fois avec les mêmes données source produit le même résultat sans créer de doublons."
  },
  {
    question: "Quelle est la différence entre INSERT OVERWRITE et CREATE OR REPLACE TABLE ?",
    options: [
      "Il n'y a pas de différence",
      "INSERT OVERWRITE est plus rapide",
      "INSERT OVERWRITE ne peut pas changer le schéma, CREATE OR REPLACE le peut",
      "CREATE OR REPLACE ne garde pas l'historique"
    ],
    correctIndex: 2,
    explanation: "INSERT OVERWRITE remplace les données mais conserve le schéma existant de la table — si les nouvelles données ne correspondent pas au schéma, l'opération échoue. CREATE OR REPLACE TABLE recrée la table entièrement et peut donc changer le schéma."
  },
  {
    question: "Pourquoi INSERT INTO peut être dangereux ?",
    options: [
      "Il supprime les données existantes",
      "Il crée des doublons si exécuté plusieurs fois",
      "Il modifie le schéma de la table",
      "Il verrouille la table pendant l'opération"
    ],
    correctIndex: 1,
    explanation: "INSERT INTO ajoute des lignes sans vérifier si elles existent déjà. Si on exécute la même commande INSERT INTO plusieurs fois, les mêmes données sont ajoutées à chaque fois, créant des doublons. Ce n'est pas idempotent."
  },
  {
    question: "Quel est l'avantage de COPY INTO ?",
    options: [
      "Il est plus rapide que tous les autres modes d'ingestion",
      "Il permet de modifier les données pendant l'ingestion",
      "Il est idempotent et ne charge pas les fichiers déjà ingérés",
      "Il fonctionne uniquement avec les fichiers CSV"
    ],
    correctIndex: 2,
    explanation: "COPY INTO est idempotent : il garde une trace des fichiers déjà chargés et ne les recharge pas lors d'exécutions ultérieures. C'est idéal pour l'ingestion incrémentale de fichiers depuis un data lake."
  }
];

const exercises: LessonExercise[] = [
  {
    id: "implementer-merge",
    title: "Implémenter un MERGE INTO",
    description: "Écrivez une requête MERGE pour effectuer un upsert de données depuis une table source vers une table cible.",
    difficulty: "moyen",
    type: "code",
    prompt: "Créez une table 'products' (cible) et une table 'products_updates' (source). Écrivez un MERGE INTO qui met à jour les produits existants et insère les nouveaux.",
    hints: [
      "Utilisez ON pour définir la condition de correspondance (ex: par product_id)",
      "WHEN MATCHED THEN UPDATE SET pour mettre à jour les lignes existantes",
      "WHEN NOT MATCHED THEN INSERT pour insérer les nouvelles lignes"
    ],
    solution: {
      code: `-- Créer la table cible\nCREATE OR REPLACE TABLE products (\n  product_id INT,\n  name STRING,\n  price DECIMAL(10,2),\n  updated_at TIMESTAMP\n);\n\nINSERT INTO products VALUES\n  (1, 'Laptop', 999.99, current_timestamp()),\n  (2, 'Mouse', 29.99, current_timestamp());\n\n-- Créer la table source avec des mises à jour\nCREATE OR REPLACE TABLE products_updates (\n  product_id INT,\n  name STRING,\n  price DECIMAL(10,2),\n  updated_at TIMESTAMP\n);\n\nINSERT INTO products_updates VALUES\n  (2, 'Mouse Pro', 49.99, current_timestamp()),\n  (3, 'Keyboard', 79.99, current_timestamp());\n\n-- MERGE INTO : upsert\nMERGE INTO products AS target\nUSING products_updates AS source\nON target.product_id = source.product_id\nWHEN MATCHED THEN\n  UPDATE SET\n    target.name = source.name,\n    target.price = source.price,\n    target.updated_at = source.updated_at\nWHEN NOT MATCHED THEN\n  INSERT (product_id, name, price, updated_at)\n  VALUES (source.product_id, source.name, source.price, source.updated_at);`,
      language: "sql",
      explanation: "Le MERGE compare chaque ligne de la source avec la cible via product_id. Le produit 2 (Mouse) est mis à jour avec le nouveau nom et prix. Le produit 3 (Keyboard) est inséré car il n'existe pas dans la cible. Cette opération est idempotente."
    }
  },
  {
    id: "comparer-insertions",
    title: "Comparer les méthodes d'insertion",
    description: "Expliquez quand utiliser chaque méthode d'écriture dans une table Delta.",
    difficulty: "difficile",
    type: "reflexion",
    prompt: "Pour chacun des scénarios suivants, indiquez la méthode la plus appropriée (INSERT INTO, INSERT OVERWRITE, CREATE OR REPLACE, MERGE INTO, COPY INTO) et justifiez votre choix :\n1. Charger des fichiers CSV quotidiens depuis un bucket S3\n2. Recalculer un rapport agrégé chaque nuit\n3. Synchroniser une table avec un système externe\n4. Ajouter des logs d'événements en temps réel",
    hints: [
      "Réfléchissez à l'idempotence : que se passe-t-il si le job est relancé ?",
      "Pensez au schéma : peut-il évoluer ou doit-il rester fixe ?",
      "Considérez le volume de données et la fréquence d'exécution"
    ],
    solution: {
      explanation: "1. COPY INTO — Idéal pour l'ingestion incrémentale de fichiers, idempotent, ne recharge pas les fichiers déjà traités.\n2. CREATE OR REPLACE TABLE — Pour un rapport recalculé entièrement, permet de changer le schéma si nécessaire.\n3. MERGE INTO — Parfait pour la synchronisation : met à jour les existants, insère les nouveaux, peut aussi supprimer.\n4. INSERT INTO — Pour l'ajout pur de données (append), adapté aux logs où les doublons sont acceptables ou gérés en amont."
    }
  }
];

export default function TransformationsDonneesPage() {
  return (
    <div className="flex min-h-[calc(100vh-4rem)]">
      <Sidebar currentPath="/modules/2-3-transformations-donnees" />

      <main className="flex-1 overflow-y-auto">
        <div className="max-w-4xl mx-auto px-6 py-10 lg:px-10">
          {/* Header */}
          <div className="mb-10">
            <div className="flex items-center gap-3 mb-3">
              <span className="inline-flex items-center px-3 py-1 rounded-full text-xs font-semibold bg-blue-100 text-blue-800">
                Module 2
              </span>
              <span className="text-sm text-[var(--color-text-light)]">
                Leçon 2.3
              </span>
            </div>
            <h1 className="text-3xl font-bold text-[var(--color-text)] mb-3">
              Transformations de Données
            </h1>
            <p className="text-lg text-[var(--color-text-light)] leading-relaxed">
              Maîtrisez les différentes méthodes d&apos;écriture dans les tables
              Delta : CREATE OR REPLACE, INSERT OVERWRITE, INSERT INTO, MERGE
              INTO et COPY INTO. Comprenez leurs cas d&apos;usage et leurs
              différences.
            </p>
          </div>

          {/* Content */}
          <section className="space-y-8">
            {/* Vue d'ensemble */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Méthodes d&apos;écriture dans les tables Delta
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Databricks offre plusieurs méthodes pour écrire des données dans
                les tables Delta. Chaque méthode a un comportement spécifique en
                termes de <strong>remplacement</strong>,{" "}
                <strong>ajout</strong>, <strong>idempotence</strong> et{" "}
                <strong>gestion du schéma</strong>. Il est essentiel de choisir
                la bonne méthode selon votre cas d&apos;usage.
              </p>
            </div>

            {/* CREATE OR REPLACE TABLE */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                1. CREATE OR REPLACE TABLE (CRAS)
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Cette commande <strong>supprime et recrée</strong> la table
                complètement. Le schéma de la table peut changer car la table
                est entièrement remplacée. L&apos;historique de la table est
                conservé (Delta garde un log des versions).
              </p>
              <CodeBlock
                language="sql"
                title="CREATE OR REPLACE TABLE"
                code={`-- Remplace entièrement la table (schéma + données)
CREATE OR REPLACE TABLE report_ventes
AS SELECT
  product_id,
  product_name,
  SUM(quantity) AS total_qty,
  SUM(amount) AS total_amount
FROM raw_ventes
GROUP BY product_id, product_name;`}
              />
            </div>

            {/* INSERT OVERWRITE */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                2. INSERT OVERWRITE
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                <strong>INSERT OVERWRITE</strong> remplace les données de la
                table sans modifier son schéma. C&apos;est une opération{" "}
                <strong>atomique</strong> : si elle échoue, les données
                originales restent intactes. Contrairement à CREATE OR REPLACE,
                le schéma de la table <strong>ne peut pas changer</strong> — les
                nouvelles données doivent correspondre au schéma existant.
              </p>
              <CodeBlock
                language="sql"
                title="INSERT OVERWRITE"
                code={`-- Remplace les données, conserve le schéma existant
INSERT OVERWRITE report_ventes
SELECT
  product_id,
  product_name,
  SUM(quantity) AS total_qty,
  SUM(amount) AS total_amount
FROM raw_ventes
GROUP BY product_id, product_name;`}
              />

              <div className="overflow-x-auto my-6">
                <table className="w-full border-collapse border border-gray-200 rounded-lg text-sm">
                  <thead>
                    <tr className="bg-gray-50">
                      <th className="border border-gray-200 px-4 py-3 text-left font-semibold text-[var(--color-text)]">
                        Caractéristique
                      </th>
                      <th className="border border-gray-200 px-4 py-3 text-left font-semibold text-[var(--color-text)]">
                        CREATE OR REPLACE TABLE
                      </th>
                      <th className="border border-gray-200 px-4 py-3 text-left font-semibold text-[var(--color-text)]">
                        INSERT OVERWRITE
                      </th>
                    </tr>
                  </thead>
                  <tbody className="text-[var(--color-text-light)]">
                    <tr>
                      <td className="border border-gray-200 px-4 py-3 font-medium">
                        Schéma
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Peut changer
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Doit correspondre au schéma existant
                      </td>
                    </tr>
                    <tr className="bg-gray-50/50">
                      <td className="border border-gray-200 px-4 py-3 font-medium">
                        Données
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Entièrement remplacées
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Entièrement remplacées
                      </td>
                    </tr>
                    <tr>
                      <td className="border border-gray-200 px-4 py-3 font-medium">
                        Table doit exister ?
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Non (la crée si nécessaire)
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Oui (erreur sinon)
                      </td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </div>

            {/* INSERT INTO */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                3. INSERT INTO
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                <strong>INSERT INTO</strong> ajoute des données à une table
                existante <strong>sans supprimer</strong> les données
                précédentes. C&apos;est un simple ajout (append). Attention :
                si vous exécutez la même commande plusieurs fois, les données
                seront <strong>dupliquées</strong>.
              </p>
              <CodeBlock
                language="sql"
                title="INSERT INTO"
                code={`-- Ajouter des données (append)
INSERT INTO clients VALUES
  (101, 'Charlie', 'charlie@example.com'),
  (102, 'Diana', 'diana@example.com');

-- Ajouter depuis une requête
INSERT INTO report_ventes
SELECT product_id, product_name, SUM(quantity), SUM(amount)
FROM new_ventes
GROUP BY product_id, product_name;`}
              />

              <InfoBox type="warning" title="Attention aux doublons">
                <p>
                  <strong>INSERT INTO</strong> ne vérifie pas l&apos;existence
                  des données avant l&apos;insertion. Si vous exécutez la même
                  commande INSERT INTO deux fois, les mêmes lignes seront
                  insérées en double. Pour éviter les doublons, utilisez{" "}
                  <strong>MERGE INTO</strong> à la place.
                </p>
              </InfoBox>
            </div>

            {/* MERGE INTO */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                4. MERGE INTO — Upserts
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                <strong>MERGE INTO</strong> est la commande la plus puissante
                pour les opérations <strong>upsert</strong> (update + insert).
                Elle permet de comparer une table cible avec une source, puis
                d&apos;appliquer des actions conditionnelles : mettre à jour les
                lignes existantes, insérer les nouvelles, ou supprimer selon des
                conditions.
              </p>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                C&apos;est une opération <strong>idempotente</strong> : exécuter
                MERGE plusieurs fois avec les mêmes données source ne crée pas
                de doublons.
              </p>
              <CodeBlock
                language="sql"
                title="MERGE INTO — Syntaxe complète"
                code={`-- MERGE INTO pour upsert
MERGE INTO target_table AS target
USING source_table AS source
ON target.id = source.id

-- Si la ligne existe déjà → mise à jour
WHEN MATCHED THEN
  UPDATE SET *

-- Si la ligne n'existe pas → insertion
WHEN NOT MATCHED THEN
  INSERT *;`}
              />

              <CodeBlock
                language="sql"
                title="MERGE avec conditions avancées"
                code={`-- MERGE avec conditions spécifiques
MERGE INTO clients AS c
USING updates AS u
ON c.id = u.id

-- Mise à jour conditionnelle
WHEN MATCHED AND u.updated_at > c.updated_at THEN
  UPDATE SET
    c.name = u.name,
    c.email = u.email,
    c.updated_at = u.updated_at

-- Suppression conditionnelle
WHEN MATCHED AND u.is_deleted = true THEN
  DELETE

-- Insertion des nouvelles lignes
WHEN NOT MATCHED THEN
  INSERT (id, name, email, updated_at)
  VALUES (u.id, u.name, u.email, u.updated_at);`}
              />

              <InfoBox type="important" title="MERGE est idempotent">
                <p>
                  Contrairement à INSERT INTO, MERGE INTO est{" "}
                  <strong>idempotent</strong>. Vous pouvez l&apos;exécuter
                  plusieurs fois avec les mêmes données sources sans créer de
                  doublons. C&apos;est la méthode recommandée pour la{" "}
                  <strong>déduplication</strong> et les{" "}
                  <strong>mises à jour incrémentales</strong>.
                </p>
              </InfoBox>
            </div>

            {/* Déduplication avec MERGE */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Déduplication avec MERGE
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Une technique courante consiste à utiliser MERGE pour insérer
                uniquement les lignes qui n&apos;existent pas encore dans la
                table cible. C&apos;est idéal pour l&apos;ingestion de lots
                (batch) où les mêmes données pourraient être envoyées plusieurs
                fois.
              </p>
              <CodeBlock
                language="sql"
                title="Déduplication avec MERGE"
                code={`-- Insérer uniquement les nouvelles lignes (pas de doublons)
MERGE INTO target_table AS t
USING new_data AS n
ON t.id = n.id

-- Ne rien faire si la ligne existe déjà
WHEN NOT MATCHED THEN
  INSERT *;

-- Avec déduplication dans la source elle-même
MERGE INTO target_table AS t
USING (
  SELECT DISTINCT * FROM new_data
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1
) AS n
ON t.id = n.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;`}
              />
            </div>

            {/* COPY INTO */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                5. COPY INTO — Ingestion Idempotente
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                <strong>COPY INTO</strong> est conçu pour l&apos;ingestion
                incrémentale de fichiers depuis un emplacement externe (cloud
                storage). Sa caractéristique principale est qu&apos;il est{" "}
                <strong>idempotent</strong> : il garde une trace des fichiers
                déjà chargés et ne les charge pas une seconde fois.
              </p>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                C&apos;est une solution idéale pour charger progressivement des
                fichiers qui arrivent dans un répertoire, sans risque de
                duplication.
              </p>
              <CodeBlock
                language="sql"
                title="COPY INTO"
                code={`-- Charger des fichiers CSV
COPY INTO target_table
FROM '/mnt/data/landing/csv_files'
FILEFORMAT = CSV
FORMAT_OPTIONS (
  'header' = 'true',
  'inferSchema' = 'true',
  'delimiter' = ','
)
COPY_OPTIONS ('mergeSchema' = 'true');

-- Charger des fichiers JSON
COPY INTO target_table
FROM '/mnt/data/landing/json_files'
FILEFORMAT = JSON;

-- Charger des fichiers Parquet
COPY INTO target_table
FROM '/mnt/data/landing/parquet_files'
FILEFORMAT = PARQUET;`}
              />

              <InfoBox type="tip" title="COPY INTO pour le chargement incrémental">
                <p>
                  <strong>COPY INTO</strong> est parfait pour les scénarios de
                  chargement incrémental. Il est plus simple à configurer
                  qu&apos;Auto Loader mais moins performant pour de très gros
                  volumes. Utilisez-le quand vous avez des milliers de fichiers ;
                  pour des millions de fichiers, préférez{" "}
                  <strong>Auto Loader</strong> (leçon 3.2).
                </p>
              </InfoBox>
            </div>

            {/* Résumé */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Résumé des méthodes d&apos;écriture
              </h2>
              <div className="overflow-x-auto mb-6">
                <table className="w-full border-collapse border border-gray-200 rounded-lg text-sm">
                  <thead>
                    <tr className="bg-gray-50">
                      <th className="border border-gray-200 px-4 py-3 text-left font-semibold text-[var(--color-text)]">
                        Méthode
                      </th>
                      <th className="border border-gray-200 px-4 py-3 text-left font-semibold text-[var(--color-text)]">
                        Comportement
                      </th>
                      <th className="border border-gray-200 px-4 py-3 text-left font-semibold text-[var(--color-text)]">
                        Idempotent
                      </th>
                      <th className="border border-gray-200 px-4 py-3 text-left font-semibold text-[var(--color-text)]">
                        Cas d&apos;usage
                      </th>
                    </tr>
                  </thead>
                  <tbody className="text-[var(--color-text-light)]">
                    <tr>
                      <td className="border border-gray-200 px-4 py-3 font-medium">
                        CREATE OR REPLACE
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Remplacement total
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        ✅ Oui
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Rebuilds complets
                      </td>
                    </tr>
                    <tr className="bg-gray-50/50">
                      <td className="border border-gray-200 px-4 py-3 font-medium">
                        INSERT OVERWRITE
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Remplacement données
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        ✅ Oui
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Rafraîchissement sans changement de schéma
                      </td>
                    </tr>
                    <tr>
                      <td className="border border-gray-200 px-4 py-3 font-medium">
                        INSERT INTO
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Ajout (append)
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        ❌ Non
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Ajout de nouvelles données
                      </td>
                    </tr>
                    <tr className="bg-gray-50/50">
                      <td className="border border-gray-200 px-4 py-3 font-medium">
                        MERGE INTO
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Upsert conditionnel
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        ✅ Oui
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Mises à jour incrémentales, CDC
                      </td>
                    </tr>
                    <tr>
                      <td className="border border-gray-200 px-4 py-3 font-medium">
                        COPY INTO
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Ingestion de fichiers
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        ✅ Oui
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Chargement incrémental depuis le cloud
                      </td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </div>
          </section>

          {/* Quiz */}
          <Quiz
            lessonSlug="2-3-transformations-donnees"
            title="Quiz — Transformations de Données"
            questions={quizQuestions}
          />

          {/* Exercices */}
          <LessonExercises
            lessonSlug="2-3-transformations-donnees"
            exercises={exercises}
          />

          {/* Bouton de complétion */}
          <LessonCompleteButton lessonSlug="2-3-transformations-donnees" />

          {/* Navigation */}
          <div className="flex items-center justify-between mt-12 pt-6 border-t border-gray-200">
            <Link
              href="/modules/2-2-vues-ctes"
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
              Leçon précédente : Vues et CTEs
            </Link>
            <Link
              href="/modules/2-4-fonctions-sql-avancees"
              className="inline-flex items-center gap-2 px-5 py-2.5 bg-[#ff3621] text-white rounded-lg font-medium hover:bg-[#e02e1a] transition-colors"
            >
              Leçon suivante : Fonctions SQL Avancées
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
