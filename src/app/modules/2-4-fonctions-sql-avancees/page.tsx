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
    question: "Que fait la fonction FILTER sur un tableau ?",
    options: [
      "Elle supprime le tableau de la table",
      "Elle retourne un nouveau tableau contenant seulement les éléments qui satisfont la condition",
      "Elle trie les éléments du tableau",
      "Elle compte le nombre d'éléments du tableau"
    ],
    correctIndex: 1,
    explanation: "FILTER prend un tableau et une fonction lambda en paramètre, puis retourne un nouveau tableau ne contenant que les éléments pour lesquels la condition lambda est vraie. Le tableau original n'est pas modifié."
  },
  {
    question: "Que fait TRANSFORM sur un tableau ?",
    options: [
      "Elle filtre les éléments du tableau",
      "Elle trie le tableau par ordre croissant",
      "Elle applique une transformation à chaque élément du tableau",
      "Elle convertit le tableau en chaîne de caractères"
    ],
    correctIndex: 2,
    explanation: "TRANSFORM applique une fonction lambda à chaque élément du tableau et retourne un nouveau tableau avec les résultats. Par exemple, TRANSFORM(array, x -> x * 2) double chaque élément."
  },
  {
    question: "Quel est l'inconvénient principal des UDFs (User Defined Functions) ?",
    options: [
      "Elles ne peuvent pas être utilisées dans les requêtes SQL",
      "Elles sont limitées à 10 lignes de code",
      "Elles sont moins performantes que les fonctions built-in car elles ne sont pas optimisées par Catalyst",
      "Elles ne supportent que les types numériques"
    ],
    correctIndex: 2,
    explanation: "Les UDFs sont exécutées ligne par ligne et ne bénéficient pas des optimisations du moteur Catalyst de Spark. Elles impliquent aussi une sérialisation/désérialisation des données, ce qui les rend plus lentes que les fonctions natives."
  },
  {
    question: "À quoi sert PIVOT ?",
    options: [
      "À fusionner deux tables",
      "À transformer des lignes en colonnes pour créer des tableaux croisés",
      "À supprimer les colonnes vides",
      "À trier les données par plusieurs colonnes"
    ],
    correctIndex: 1,
    explanation: "PIVOT transforme les valeurs d'une colonne en noms de colonnes, créant un tableau croisé dynamique. C'est utile pour résumer des données, par exemple transformer des mois en colonnes pour voir les ventes par mois."
  },
  {
    question: "Quelle est la différence entre une UDF SQL et une UDF Python ?",
    options: [
      "Les UDF SQL sont plus lentes",
      "Les UDF Python ne peuvent pas retourner de valeurs",
      "UDF SQL est définie avec CREATE FUNCTION, UDF Python avec spark.udf.register()",
      "Il n'y a pas de différence"
    ],
    correctIndex: 2,
    explanation: "Les UDF SQL sont créées avec la commande CREATE FUNCTION directement en SQL. Les UDF Python sont définies comme des fonctions Python puis enregistrées avec spark.udf.register() pour être utilisables en SQL."
  }
];

const exercises: LessonExercise[] = [
  {
    id: "higher-order-functions",
    title: "Utiliser les higher-order functions",
    description: "Pratiquez l'utilisation de FILTER, TRANSFORM et EXISTS sur des colonnes de type tableau.",
    difficulty: "moyen",
    type: "code",
    prompt: "Créez une table avec une colonne de type array contenant des scores. Utilisez FILTER pour garder les scores > 80, TRANSFORM pour convertir chaque score en pourcentage, et EXISTS pour vérifier si un score parfait (100) existe.",
    hints: [
      "Créez la table avec : CREATE TABLE students (name STRING, scores ARRAY<INT>)",
      "FILTER syntax : FILTER(scores, x -> x > 80)",
      "TRANSFORM syntax : TRANSFORM(scores, x -> x / 100.0)"
    ],
    solution: {
      code: `-- Créer la table avec des tableaux\nCREATE OR REPLACE TABLE students (\n  name STRING,\n  scores ARRAY<INT>\n);\n\nINSERT INTO students VALUES\n  ('Alice', ARRAY(85, 92, 78, 100, 65)),\n  ('Bob', ARRAY(70, 88, 95, 60, 82)),\n  ('Charlie', ARRAY(100, 98, 87, 92, 100));\n\n-- FILTER : garder les scores > 80\nSELECT\n  name,\n  FILTER(scores, x -> x > 80) AS high_scores\nFROM students;\n\n-- TRANSFORM : convertir en pourcentage\nSELECT\n  name,\n  TRANSFORM(scores, x -> CONCAT(CAST(x AS STRING), '%')) AS score_pct\nFROM students;\n\n-- EXISTS : vérifier si un score parfait existe\nSELECT\n  name,\n  EXISTS(scores, x -> x = 100) AS has_perfect_score\nFROM students;`,
      language: "sql",
      explanation: "FILTER retourne un sous-tableau des éléments satisfaisant la condition. TRANSFORM applique une opération à chaque élément. EXISTS retourne un booléen indiquant si au moins un élément satisfait la condition."
    }
  },
  {
    id: "creer-udf",
    title: "Créer une UDF personnalisée",
    description: "Créez des UDFs en SQL et en Python pour une transformation personnalisée.",
    difficulty: "moyen",
    type: "code",
    prompt: "Créez une UDF SQL qui catégorise un montant en 'Faible' (< 100), 'Moyen' (100-500) ou 'Élevé' (> 500). Créez ensuite l'équivalent en Python.",
    hints: [
      "En SQL, utilisez CREATE FUNCTION avec un RETURN et des CASE WHEN",
      "En Python, définissez une fonction Python puis enregistrez-la avec spark.udf.register()",
      "Testez vos UDFs avec SELECT udf_name(valeur)"
    ],
    solution: {
      code: `-- UDF SQL\nCREATE OR REPLACE FUNCTION categorize_amount(amount DOUBLE)\nRETURNS STRING\nRETURN CASE\n  WHEN amount < 100 THEN 'Faible'\n  WHEN amount <= 500 THEN 'Moyen'\n  ELSE 'Élevé'\nEND;\n\n-- Tester la UDF SQL\nSELECT categorize_amount(50);    -- Faible\nSELECT categorize_amount(250);   -- Moyen\nSELECT categorize_amount(1000);  -- Élevé\n\n-- UDF Python (dans une cellule Python)\n-- def categorize_amount_py(amount):\n--     if amount < 100:\n--         return 'Faible'\n--     elif amount <= 500:\n--         return 'Moyen'\n--     else:\n--         return 'Élevé'\n--\n-- spark.udf.register('categorize_amount_py', categorize_amount_py)\n--\n-- Tester : SELECT categorize_amount_py(250)`,
      language: "sql",
      explanation: "La UDF SQL utilise CREATE FUNCTION avec RETURN et du SQL standard (CASE WHEN). La UDF Python est une fonction Python classique enregistrée via spark.udf.register(), ce qui la rend utilisable dans les requêtes SQL."
    }
  }
];

export default function FonctionsSQLAvanceesPage() {
  return (
    <div className="flex min-h-[calc(100vh-4rem)]">
      <Sidebar currentPath="/modules/2-4-fonctions-sql-avancees" />

      <main className="flex-1 overflow-y-auto">
        <div className="max-w-4xl mx-auto px-6 py-10 lg:px-10">
          {/* Header */}
          <div className="mb-10">
            <div className="flex items-center gap-3 mb-3">
              <span className="inline-flex items-center px-3 py-1 rounded-full text-xs font-semibold bg-blue-100 text-blue-800">
                Module 2
              </span>
              <span className="text-sm text-[var(--color-text-light)]">
                Leçon 2.4
              </span>
            </div>
            <h1 className="text-3xl font-bold text-[var(--color-text)] mb-3">
              Fonctions SQL Avancées
            </h1>
            <p className="text-lg text-[var(--color-text-light)] leading-relaxed">
              Explorez les fonctions d&apos;ordre supérieur (FILTER, TRANSFORM,
              EXISTS), les fonctions définies par l&apos;utilisateur (UDFs), la
              manipulation de types complexes et le PIVOT pour transformer vos
              données.
            </p>
          </div>

          {/* Content */}
          <section className="space-y-8">
            {/* Higher-order functions */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Fonctions d&apos;ordre supérieur
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Les <strong>fonctions d&apos;ordre supérieur</strong>{" "}
                (higher-order functions) permettent de manipuler directement des
                colonnes de type <strong>array</strong> sans avoir à les
                décomposer avec EXPLODE. Elles prennent une fonction anonyme
                (lambda) en paramètre, exprimée sous la forme{" "}
                <code className="bg-gray-100 px-1.5 py-0.5 rounded text-sm font-mono">
                  x -&gt; expression
                </code>
                .
              </p>

              <InfoBox type="info" title="Clé pour l'examen">
                <p>
                  Les fonctions d&apos;ordre supérieur sont un sujet{" "}
                  <strong>fréquemment testé</strong> à la certification
                  Databricks Data Engineer Associate. Assurez-vous de bien
                  comprendre la syntaxe et les différences entre FILTER,
                  TRANSFORM et EXISTS.
                </p>
              </InfoBox>
            </div>

            {/* FILTER */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                FILTER — Filtrer les éléments d&apos;un tableau
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                <strong>FILTER</strong> retourne un nouveau tableau contenant
                uniquement les éléments qui satisfont la condition de la
                fonction lambda. Le tableau original n&apos;est pas modifié.
              </p>
              <CodeBlock
                language="sql"
                title="Fonction FILTER"
                code={`-- Filtrer les valeurs positives d'un tableau
SELECT
  order_id,
  items,
  FILTER(items, x -> x > 0) AS items_positifs
FROM orders;

-- Filtrer les éléments d'un tableau de structs
SELECT
  customer_id,
  FILTER(purchases, p -> p.amount > 100) AS achats_importants
FROM customers;

-- Filtrer les chaînes non vides
SELECT FILTER(array('hello', '', 'world', ''), x -> x != '') AS non_vides;
-- Résultat : ['hello', 'world']`}
              />
            </div>

            {/* TRANSFORM */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                TRANSFORM — Transformer chaque élément
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                <strong>TRANSFORM</strong> applique une fonction lambda à
                chaque élément du tableau et retourne un nouveau tableau avec
                les résultats. C&apos;est l&apos;équivalent d&apos;un{" "}
                <code className="bg-gray-100 px-1.5 py-0.5 rounded text-sm font-mono">
                  map()
                </code>{" "}
                en programmation fonctionnelle.
              </p>
              <CodeBlock
                language="sql"
                title="Fonction TRANSFORM"
                code={`-- Doubler chaque valeur d'un tableau
SELECT TRANSFORM(array(1, 2, 3, 4), x -> x * 2) AS doubles;
-- Résultat : [2, 4, 6, 8]

-- Mettre en majuscules
SELECT TRANSFORM(array('alice', 'bob'), x -> UPPER(x)) AS noms_upper;
-- Résultat : ['ALICE', 'BOB']

-- Transformer un tableau de prix (ajouter la TVA)
SELECT
  product_name,
  prices,
  TRANSFORM(prices, p -> ROUND(p * 1.20, 2)) AS prices_ttc
FROM products;`}
              />
            </div>

            {/* EXISTS */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                EXISTS — Tester l&apos;existence d&apos;un élément
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                <strong>EXISTS</strong> retourne{" "}
                <code className="bg-gray-100 px-1.5 py-0.5 rounded text-sm font-mono">
                  true
                </code>{" "}
                si au moins un élément du tableau satisfait la condition de la
                lambda, et{" "}
                <code className="bg-gray-100 px-1.5 py-0.5 rounded text-sm font-mono">
                  false
                </code>{" "}
                sinon.
              </p>
              <CodeBlock
                language="sql"
                title="Fonction EXISTS"
                code={`-- Vérifier si un tableau contient une valeur > 10
SELECT EXISTS(array(1, 5, 12, 3), x -> x > 10) AS has_value_above_10;
-- Résultat : true

-- Filtrer les commandes contenant des articles chers
SELECT order_id, items
FROM orders
WHERE EXISTS(items, i -> i.price > 500);

-- Vérifier si un tableau contient une chaîne spécifique
SELECT EXISTS(array('SQL', 'Python', 'Scala'), x -> x = 'Python') AS has_python;
-- Résultat : true`}
              />
            </div>

            {/* Types complexes */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Manipulation de types complexes
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Databricks supporte les types complexes comme les{" "}
                <strong>arrays</strong>, les <strong>maps</strong> et les{" "}
                <strong>structs</strong>. Voici les fonctions les plus utiles
                pour les manipuler.
              </p>
              <CodeBlock
                language="sql"
                title="Fonctions sur les types complexes"
                code={`-- EXPLODE : transforme chaque élément d'un tableau en ligne
SELECT order_id, EXPLODE(items) AS item
FROM orders;

-- COLLECT_SET : agrège des valeurs uniques en tableau
SELECT customer_id, COLLECT_SET(product_category) AS categories
FROM purchases
GROUP BY customer_id;

-- FLATTEN : aplatir un tableau de tableaux
SELECT FLATTEN(array(array(1, 2), array(3, 4))) AS flat;
-- Résultat : [1, 2, 3, 4]

-- ARRAY_DISTINCT : supprimer les doublons d'un tableau
SELECT ARRAY_DISTINCT(array(1, 2, 2, 3, 3, 3)) AS unique_values;
-- Résultat : [1, 2, 3]

-- Accéder à un élément d'un struct
SELECT address.city, address.zip_code
FROM customers;

-- Accéder à une valeur dans un map
SELECT preferences['theme'] AS theme
FROM user_settings;`}
              />
            </div>

            {/* UDFs SQL */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                UDFs SQL — Fonctions Définies par l&apos;Utilisateur
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Les <strong>UDFs SQL</strong> permettent de créer des fonctions
                réutilisables directement en SQL. Elles sont enregistrées dans
                le metastore et peuvent être utilisées par tous les utilisateurs
                ayant accès à la base de données.
              </p>
              <CodeBlock
                language="sql"
                title="Créer une UDF SQL"
                code={`-- UDF simple : doubler une valeur
CREATE OR REPLACE FUNCTION doubler(x INT)
RETURNS INT
RETURN x * 2;

-- Utiliser la UDF
SELECT doubler(5); -- Résultat : 10
SELECT name, doubler(score) AS double_score FROM students;

-- UDF avec logique conditionnelle
CREATE OR REPLACE FUNCTION categorize_age(age INT)
RETURNS STRING
RETURN CASE
  WHEN age < 18 THEN 'Mineur'
  WHEN age < 65 THEN 'Adulte'
  ELSE 'Senior'
END;

-- UDF de calcul de TVA
CREATE OR REPLACE FUNCTION prix_ttc(prix_ht DOUBLE, taux DOUBLE DEFAULT 0.20)
RETURNS DOUBLE
RETURN ROUND(prix_ht * (1 + taux), 2);

SELECT prix_ttc(100);     -- 120.0
SELECT prix_ttc(100, 0.055); -- 105.5

-- Supprimer une UDF
DROP FUNCTION IF EXISTS doubler;`}
              />
            </div>

            {/* UDFs Python */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                UDFs Python
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Vous pouvez également créer des UDFs en Python et les
                enregistrer pour les utiliser dans des requêtes SQL. Cela permet
                d&apos;appliquer une logique plus complexe qu&apos;en SQL pur.
              </p>
              <CodeBlock
                language="python"
                title="Enregistrer une UDF Python pour SQL"
                code={`# Définir une UDF Python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Méthode 1 : avec spark.udf.register (utilisable en SQL)
def format_phone(phone):
    if phone and len(phone) == 10:
        return f"+33 {phone[1:3]} {phone[3:5]} {phone[5:7]} {phone[7:9]} {phone[9:]}"
    return phone

spark.udf.register("format_phone_sql", format_phone, StringType())

# Maintenant utilisable en SQL :
# SELECT format_phone_sql(phone) FROM clients;

# Méthode 2 : avec le décorateur @udf (utilisable en PySpark)
@udf(returnType=StringType())
def format_name(first, last):
    return f"{first.title()} {last.upper()}"

# Utilisation en PySpark
df = spark.table("clients")
df.select(format_name("first_name", "last_name").alias("full_name")).display()`}
              />

              <InfoBox type="tip" title="Performance des UDFs">
                <p>
                  Les UDFs (surtout Python) sont <strong>moins performantes</strong>{" "}
                  que les fonctions intégrées de Spark car elles ne bénéficient
                  pas de l&apos;optimisation Catalyst. Utilisez toujours les{" "}
                  <strong>fonctions built-in</strong> quand c&apos;est possible
                  (ex. : UPPER, LOWER, CONCAT, etc.). Réservez les UDFs pour
                  les logiques impossibles à exprimer avec les fonctions
                  natives.
                </p>
              </InfoBox>
            </div>

            {/* PIVOT */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                PIVOT — Pivoter les données
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                L&apos;opération <strong>PIVOT</strong> transforme des lignes en
                colonnes. C&apos;est utile pour créer des tableaux croisés
                dynamiques directement en SQL, par exemple pour afficher des
                ventes par catégorie en colonnes.
              </p>
              <CodeBlock
                language="sql"
                title="PIVOT — Tableau croisé dynamique"
                code={`-- Données d'origine :
-- | product | quarter | revenue |
-- | A       | Q1      | 100     |
-- | A       | Q2      | 150     |
-- | B       | Q1      | 200     |
-- | B       | Q2      | 250     |

-- PIVOT : transformer les trimestres en colonnes
SELECT * FROM ventes
PIVOT (
  SUM(revenue)
  FOR quarter IN ('Q1', 'Q2', 'Q3', 'Q4')
);

-- Résultat :
-- | product | Q1  | Q2  | Q3   | Q4   |
-- | A       | 100 | 150 | NULL | NULL |
-- | B       | 200 | 250 | NULL | NULL |

-- PIVOT avec plusieurs agrégations
SELECT * FROM ventes
PIVOT (
  SUM(revenue) AS total,
  COUNT(revenue) AS nb
  FOR quarter IN ('Q1', 'Q2')
);`}
              />
            </div>

            {/* Résumé */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Récapitulatif
              </h2>
              <div className="overflow-x-auto mb-6">
                <table className="w-full border-collapse border border-gray-200 rounded-lg text-sm">
                  <thead>
                    <tr className="bg-gray-50">
                      <th className="border border-gray-200 px-4 py-3 text-left font-semibold text-[var(--color-text)]">
                        Fonction
                      </th>
                      <th className="border border-gray-200 px-4 py-3 text-left font-semibold text-[var(--color-text)]">
                        Description
                      </th>
                      <th className="border border-gray-200 px-4 py-3 text-left font-semibold text-[var(--color-text)]">
                        Retour
                      </th>
                    </tr>
                  </thead>
                  <tbody className="text-[var(--color-text-light)]">
                    <tr>
                      <td className="border border-gray-200 px-4 py-3 font-mono font-medium">
                        FILTER
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Filtre les éléments selon une condition
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Array
                      </td>
                    </tr>
                    <tr className="bg-gray-50/50">
                      <td className="border border-gray-200 px-4 py-3 font-mono font-medium">
                        TRANSFORM
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Transforme chaque élément
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Array
                      </td>
                    </tr>
                    <tr>
                      <td className="border border-gray-200 px-4 py-3 font-mono font-medium">
                        EXISTS
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Teste si un élément satisfait une condition
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Boolean
                      </td>
                    </tr>
                    <tr className="bg-gray-50/50">
                      <td className="border border-gray-200 px-4 py-3 font-mono font-medium">
                        EXPLODE
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Décompose un array en lignes
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Lignes
                      </td>
                    </tr>
                    <tr>
                      <td className="border border-gray-200 px-4 py-3 font-mono font-medium">
                        COLLECT_SET
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Agrège des valeurs uniques en array
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Array
                      </td>
                    </tr>
                    <tr className="bg-gray-50/50">
                      <td className="border border-gray-200 px-4 py-3 font-mono font-medium">
                        PIVOT
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Transforme des lignes en colonnes
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Table restructurée
                      </td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </div>
          </section>

          {/* Quiz */}
          <Quiz
            lessonSlug="2-4-fonctions-sql-avancees"
            title="Quiz — Fonctions SQL Avancées"
            questions={quizQuestions}
          />

          {/* Exercices */}
          <LessonExercises
            lessonSlug="2-4-fonctions-sql-avancees"
            exercises={exercises}
          />

          {/* Bouton de complétion */}
          <LessonCompleteButton lessonSlug="2-4-fonctions-sql-avancees" />

          {/* Navigation */}
          <div className="flex items-center justify-between mt-12 pt-6 border-t border-gray-200">
            <Link
              href="/modules/2-3-transformations-donnees"
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
              Leçon précédente : Transformations de Données
            </Link>
            <Link
              href="/modules/3-1-structured-streaming"
              className="inline-flex items-center gap-2 px-5 py-2.5 bg-[#ff3621] text-white rounded-lg font-medium hover:bg-[#e02e1a] transition-colors"
            >
              Leçon suivante : Structured Streaming
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
