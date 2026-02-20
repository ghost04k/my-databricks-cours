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
    question: "Quelle est la différence entre une table managed et une table external ?",
    options: [
      "Managed : Databricks gère données + métadonnées. External : seulement métadonnées",
      "External : Databricks gère données + métadonnées. Managed : seulement métadonnées",
      "Managed et External sont identiques, seul le nom change",
      "Managed stocke les données en CSV, External en Parquet"
    ],
    correctIndex: 0,
    explanation: "En table managed, Databricks gère les données ET les métadonnées. En external, seules les métadonnées sont gérées par Databricks, les fichiers de données sont gérés par l'utilisateur à un emplacement externe."
  },
  {
    question: "Que se passe-t-il quand on DROP une table external ?",
    options: [
      "Les données et les métadonnées sont supprimées",
      "Seules les métadonnées sont supprimées, les fichiers restent",
      "Rien ne se passe, la commande échoue",
      "Les fichiers sont déplacés dans la corbeille"
    ],
    correctIndex: 1,
    explanation: "Quand on DROP une table external, seules les métadonnées du metastore sont supprimées. Les fichiers de données sous-jacents restent intacts à leur emplacement d'origine."
  },
  {
    question: "Quelle commande permet de vérifier si une table est managed ou external ?",
    options: [
      "SHOW TABLE STATUS table_name",
      "DESCRIBE EXTENDED table_name",
      "SELECT TYPE FROM table_name",
      "CHECK TABLE table_name"
    ],
    correctIndex: 1,
    explanation: "DESCRIBE EXTENDED table_name affiche les métadonnées complètes de la table, y compris le champ 'Type' qui indique MANAGED ou EXTERNAL."
  },
  {
    question: "Quelle est la différence entre DEEP CLONE et SHALLOW CLONE ?",
    options: [
      "Les deux copient les données, DEEP CLONE est juste plus rapide",
      "DEEP CLONE copie les données, SHALLOW CLONE copie seulement les métadonnées et référence les fichiers originaux",
      "SHALLOW CLONE copie les données, DEEP CLONE copie seulement les métadonnées",
      "Il n'y a pas de différence, ce sont des alias"
    ],
    correctIndex: 1,
    explanation: "DEEP CLONE effectue une copie complète des données et métadonnées. SHALLOW CLONE ne copie que les métadonnées Delta et crée des références vers les fichiers de données originaux, ce qui est plus rapide mais crée une dépendance."
  },
  {
    question: "Peut-on spécifier un schéma manuellement avec CTAS (CREATE TABLE AS SELECT) ?",
    options: [
      "Oui, en ajoutant la clause SCHEMA avant AS SELECT",
      "Oui, en utilisant CAST dans la requête SELECT",
      "Non, le schéma est déduit automatiquement de la requête SELECT",
      "Oui, avec la clause COLUMNS après le nom de la table"
    ],
    correctIndex: 2,
    explanation: "Avec CTAS, le schéma est toujours déduit automatiquement de la requête SELECT. On ne peut pas déclarer de colonnes manuellement. Pour contrôler les types, il faut utiliser CAST dans la requête SELECT elle-même."
  }
];

const exercises: LessonExercise[] = [
  {
    id: "creer-tables",
    title: "Créer des tables managed et external",
    description: "Créez une table managed et une table external, puis utilisez DESCRIBE EXTENDED pour comparer leurs propriétés.",
    difficulty: "facile",
    type: "code",
    prompt: "Créez une table managed 'employees_managed' et une table external 'employees_external' avec les colonnes id, name et department. Utilisez DESCRIBE EXTENDED pour vérifier le type de chaque table.",
    hints: [
      "Une table managed est créée simplement avec CREATE TABLE sans LOCATION",
      "Une table external utilise le mot-clé LOCATION pour spécifier le chemin des fichiers",
      "DESCRIBE EXTENDED affiche le type dans la section 'Detailed Table Information'"
    ],
    solution: {
      code: `-- Table Managed\nCREATE TABLE employees_managed (\n  id INT,\n  name STRING,\n  department STRING\n);\n\n-- Table External\nCREATE TABLE employees_external (\n  id INT,\n  name STRING,\n  department STRING\n)\nLOCATION '/mnt/data/employees_external';\n\n-- Vérifier le type\nDESCRIBE EXTENDED employees_managed;\nDESCRIBE EXTENDED employees_external;`,
      language: "sql",
      explanation: "La table managed sera de type MANAGED et stockée dans le répertoire par défaut du metastore. La table external sera de type EXTERNAL et stockée à l'emplacement spécifié avec LOCATION."
    }
  },
  {
    id: "tester-drop",
    title: "Tester le DROP",
    description: "Supprimez les deux tables et vérifiez quels fichiers de données subsistent après le DROP.",
    difficulty: "moyen",
    type: "pratique",
    prompt: "Insérez des données dans les deux tables, puis exécutez DROP TABLE sur chacune. Vérifiez si les fichiers de données existent toujours pour la table external.",
    hints: [
      "Utilisez INSERT INTO pour ajouter des données dans les tables",
      "Après le DROP, utilisez %fs ls pour vérifier si les fichiers existent encore",
      "Les fichiers de la table managed seront supprimés, ceux de la table external resteront"
    ],
    solution: {
      code: `-- Insérer des données\nINSERT INTO employees_managed VALUES (1, 'Alice', 'Engineering');\nINSERT INTO employees_external VALUES (1, 'Bob', 'Marketing');\n\n-- Supprimer les tables\nDROP TABLE employees_managed;\nDROP TABLE employees_external;\n\n-- Vérifier les fichiers (la table external devrait encore avoir ses fichiers)\n-- %fs ls '/mnt/data/employees_external'`,
      language: "sql",
      explanation: "Après le DROP, les fichiers de la table managed sont supprimés car Databricks gère le cycle de vie complet. Les fichiers de la table external restent car Databricks ne gère que les métadonnées."
    }
  }
];

export default function BasesDonneesTablesPage() {
  return (
    <div className="flex min-h-[calc(100vh-4rem)]">
      <Sidebar currentPath="/modules/2-1-bases-donnees-tables" />

      <main className="flex-1 overflow-y-auto">
        <div className="max-w-4xl mx-auto px-6 py-10 lg:px-10">
          {/* Header */}
          <div className="mb-10">
            <div className="flex items-center gap-3 mb-3">
              <span className="inline-flex items-center px-3 py-1 rounded-full text-xs font-semibold bg-blue-100 text-blue-800">
                Module 2
              </span>
              <span className="text-sm text-[var(--color-text-light)]">
                Leçon 2.1
              </span>
            </div>
            <h1 className="text-3xl font-bold text-[var(--color-text)] mb-3">
              Bases de données et Tables
            </h1>
            <p className="text-lg text-[var(--color-text-light)] leading-relaxed">
              Apprenez à créer et gérer des bases de données et des tables dans
              Databricks. Comprenez la différence entre les tables managées et
              externes, les contraintes, le clonage et les instructions CTAS.
            </p>
          </div>

          {/* Content */}
          <section className="space-y-8">
            {/* Bases de données / Schémas */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Qu&apos;est-ce qu&apos;une base de données (schéma) ?
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Dans Databricks, les termes <strong>DATABASE</strong> et{" "}
                <strong>SCHEMA</strong> sont interchangeables. Une base de
                données est un regroupement logique de tables, vues et fonctions.
                Par défaut, Databricks utilise la base de données appelée{" "}
                <code className="bg-gray-100 px-1.5 py-0.5 rounded text-sm font-mono">
                  default
                </code>
                .
              </p>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Lorsque vous créez une base de données, vous pouvez
                optionnellement spécifier un emplacement avec le mot-clé{" "}
                <strong>LOCATION</strong>. Si aucun emplacement n&apos;est
                spécifié, la base de données sera créée dans le répertoire par
                défaut du metastore.
              </p>
              <CodeBlock
                language="sql"
                title="Création d'une base de données"
                code={`-- CREATE DATABASE et CREATE SCHEMA sont identiques
CREATE DATABASE IF NOT EXISTS ma_base_de_donnees;

-- Avec un emplacement personnalisé
CREATE DATABASE IF NOT EXISTS ma_base_externe
LOCATION '/mnt/data/ma_base_externe';

-- Utiliser une base de données
USE ma_base_de_donnees;

-- Afficher les bases de données disponibles
SHOW DATABASES;`}
              />
            </div>

            {/* Tables Managées vs Externes */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Tables Managées vs Tables Externes
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Il existe deux types de tables dans Databricks :{" "}
                <strong>managées</strong> (managed) et{" "}
                <strong>externes</strong> (external/unmanaged). La différence
                fondamentale réside dans la gestion des données sous-jacentes.
              </p>

              {/* Tableau comparatif */}
              <div className="overflow-x-auto mb-6">
                <table className="w-full border-collapse border border-gray-200 rounded-lg text-sm">
                  <thead>
                    <tr className="bg-gray-50">
                      <th className="border border-gray-200 px-4 py-3 text-left font-semibold text-[var(--color-text)]">
                        Caractéristique
                      </th>
                      <th className="border border-gray-200 px-4 py-3 text-left font-semibold text-[var(--color-text)]">
                        Table Managée
                      </th>
                      <th className="border border-gray-200 px-4 py-3 text-left font-semibold text-[var(--color-text)]">
                        Table Externe
                      </th>
                    </tr>
                  </thead>
                  <tbody className="text-[var(--color-text-light)]">
                    <tr>
                      <td className="border border-gray-200 px-4 py-3 font-medium">
                        Métadonnées
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Gérées par Databricks
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Gérées par Databricks
                      </td>
                    </tr>
                    <tr className="bg-gray-50/50">
                      <td className="border border-gray-200 px-4 py-3 font-medium">
                        Fichiers de données
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Gérés par Databricks (stockés dans le metastore)
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Stockés à l&apos;emplacement spécifié par l&apos;utilisateur
                      </td>
                    </tr>
                    <tr>
                      <td className="border border-gray-200 px-4 py-3 font-medium">
                        Comportement au DROP
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Métadonnées <strong>ET</strong> données supprimées
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Seules les métadonnées sont supprimées, les données restent
                      </td>
                    </tr>
                    <tr className="bg-gray-50/50">
                      <td className="border border-gray-200 px-4 py-3 font-medium">
                        Mot-clé LOCATION
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Non spécifié
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Spécifié obligatoirement
                      </td>
                    </tr>
                  </tbody>
                </table>
              </div>

              <CodeBlock
                language="sql"
                title="Créer une table managée"
                code={`-- Table managée : Databricks gère tout
CREATE TABLE managed_table (
  id INT,
  name STRING,
  email STRING,
  created_at TIMESTAMP
);

-- Insérer des données
INSERT INTO managed_table VALUES
  (1, 'Alice', 'alice@example.com', current_timestamp()),
  (2, 'Bob', 'bob@example.com', current_timestamp());`}
              />

              <CodeBlock
                language="sql"
                title="Créer une table externe"
                code={`-- Table externe : les données sont stockées à l'emplacement spécifié
CREATE TABLE external_table (
  id INT,
  name STRING,
  value DOUBLE
)
LOCATION '/mnt/data/external_table';

-- Depuis une source existante
CREATE TABLE external_from_source
LOCATION '/mnt/data/external_source'
AS SELECT * FROM source_table;`}
              />

              <InfoBox type="important" title="Comportement du DROP">
                <p>
                  Lorsque vous supprimez une <strong>table managée</strong> avec{" "}
                  <code className="bg-red-100 px-1 py-0.5 rounded text-xs font-mono">
                    DROP TABLE
                  </code>
                  , les données sous-jacentes sont <strong>définitivement
                  supprimées</strong>. En revanche, pour une{" "}
                  <strong>table externe</strong>, seules les métadonnées sont
                  supprimées du metastore : les fichiers de données restent
                  intacts à leur emplacement d&apos;origine.
                </p>
              </InfoBox>
            </div>

            {/* CTAS */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                CTAS — CREATE TABLE AS SELECT
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                L&apos;instruction <strong>CTAS</strong> permet de créer une table
                et de la remplir en une seule opération à partir d&apos;une requête
                SELECT. C&apos;est un moyen rapide de créer des tables dérivées.
              </p>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                <strong>Attention :</strong> avec CTAS, vous ne pouvez{" "}
                <strong>pas</strong> spécifier manuellement le schéma de la
                table. Le schéma est automatiquement inféré à partir des
                résultats de la requête SELECT.
              </p>
              <CodeBlock
                language="sql"
                title="Exemples CTAS"
                code={`-- CTAS simple : le schéma est inféré automatiquement
CREATE TABLE new_table
AS SELECT id, name, UPPER(email) AS email_upper
FROM managed_table;

-- CTAS avec remplacement
CREATE OR REPLACE TABLE filtered_table
AS SELECT * FROM managed_table
WHERE id > 1;

-- CTAS ne permet PAS de spécifier les colonnes manuellement
-- Ceci est INVALIDE :
-- CREATE TABLE my_table (id INT, name STRING)
-- AS SELECT * FROM source;`}
              />
            </div>

            {/* Contraintes de table */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Contraintes de Table
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Databricks supporte deux types de contraintes sur les tables
                Delta : <strong>NOT NULL</strong> et <strong>CHECK</strong>.
                Ces contraintes garantissent l&apos;intégrité des données en
                rejetant les insertions qui ne respectent pas les règles
                définies.
              </p>
              <CodeBlock
                language="sql"
                title="Ajouter des contraintes"
                code={`-- Ajouter une contrainte NOT NULL
ALTER TABLE managed_table ALTER COLUMN id SET NOT NULL;

-- Ajouter une contrainte CHECK
ALTER TABLE managed_table
ADD CONSTRAINT valid_id CHECK (id > 0);

-- Ajouter une contrainte CHECK plus complexe
ALTER TABLE managed_table
ADD CONSTRAINT valid_email CHECK (email LIKE '%@%.%');

-- Supprimer une contrainte
ALTER TABLE managed_table DROP CONSTRAINT valid_email;`}
              />
            </div>

            {/* Clonage de tables */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Clonage de Tables : DEEP CLONE vs SHALLOW CLONE
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Le clonage permet de copier une table Delta. Il existe deux
                types de clones :
              </p>
              <ul className="list-disc list-inside space-y-2 text-[var(--color-text-light)] mb-4">
                <li>
                  <strong>DEEP CLONE :</strong> copie complète des données et
                  des métadonnées. Le clone est totalement indépendant de la
                  source. Les modifications sur l&apos;un n&apos;affectent pas
                  l&apos;autre.
                </li>
                <li>
                  <strong>SHALLOW CLONE :</strong> copie uniquement les
                  métadonnées et crée des références vers les fichiers de
                  données de la source. Plus rapide à créer, mais dépend de la
                  table source.
                </li>
              </ul>
              <CodeBlock
                language="sql"
                title="Clonage de tables"
                code={`-- Deep Clone : copie complète et indépendante
CREATE TABLE table_clone_deep
DEEP CLONE source_table;

-- Shallow Clone : copie légère avec références
CREATE TABLE table_clone_shallow
SHALLOW CLONE source_table;`}
              />
            </div>

            {/* DESCRIBE EXTENDED */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Inspecter les métadonnées : DESCRIBE EXTENDED
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                La commande <strong>DESCRIBE EXTENDED</strong> affiche des
                informations détaillées sur une table, y compris son type
                (managée ou externe), son emplacement, son format, et d&apos;autres
                métadonnées utiles.
              </p>
              <CodeBlock
                language="sql"
                title="Métadonnées d'une table"
                code={`-- Afficher les détails complets d'une table
DESCRIBE EXTENDED managed_table;

-- Afficher uniquement le schéma
DESCRIBE managed_table;

-- Afficher l'historique des modifications (Delta)
DESCRIBE HISTORY managed_table;`}
              />

              <InfoBox type="tip" title="Astuce pour l'examen">
                <p>
                  Utilisez{" "}
                  <code className="bg-emerald-100 px-1 py-0.5 rounded text-xs font-mono">
                    DESCRIBE EXTENDED
                  </code>{" "}
                  pour vérifier si une table est <strong>managée</strong> ou{" "}
                  <strong>externe</strong>. Regardez le champ{" "}
                  <code className="bg-emerald-100 px-1 py-0.5 rounded text-xs font-mono">
                    Type
                  </code>{" "}
                  dans les résultats : il indiquera{" "}
                  <code className="bg-emerald-100 px-1 py-0.5 rounded text-xs font-mono">
                    MANAGED
                  </code>{" "}
                  ou{" "}
                  <code className="bg-emerald-100 px-1 py-0.5 rounded text-xs font-mono">
                    EXTERNAL
                  </code>
                  . C&apos;est une question fréquente à la certification !
                </p>
              </InfoBox>
            </div>
          </section>

          {/* Quiz */}
          <Quiz
            lessonSlug="2-1-bases-donnees-tables"
            title="Quiz — Bases de données et Tables"
            questions={quizQuestions}
          />

          {/* Exercices */}
          <LessonExercises
            lessonSlug="2-1-bases-donnees-tables"
            exercises={exercises}
          />

          {/* Bouton de complétion */}
          <LessonCompleteButton lessonSlug="2-1-bases-donnees-tables" />

          {/* Navigation */}
          <div className="flex items-center justify-between mt-12 pt-6 border-t border-gray-200">
            <Link
              href="/modules/1-1-bases-notebooks"
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
              Leçon précédente : Bases des Notebooks
            </Link>
            <Link
              href="/modules/2-2-vues-ctes"
              className="inline-flex items-center gap-2 px-5 py-2.5 bg-[#ff3621] text-white rounded-lg font-medium hover:bg-[#e02e1a] transition-colors"
            >
              Leçon suivante : Vues et CTEs
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
