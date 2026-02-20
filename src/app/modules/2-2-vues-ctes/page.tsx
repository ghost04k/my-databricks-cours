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
    question: "Quelle vue survit au redémarrage de la session ?",
    options: [
      "CREATE TEMP VIEW",
      "CREATE VIEW (vue stockée)",
      "CREATE GLOBAL TEMP VIEW",
      "Aucune vue ne survit au redémarrage"
    ],
    correctIndex: 1,
    explanation: "Seule la vue stockée (CREATE VIEW) est persistée dans la base de données et survit au redémarrage du cluster et de la session. Les vues temporaires et globales temporaires sont supprimées à la fin de la session."
  },
  {
    question: "Comment accède-t-on à une Global Temp View ?",
    options: [
      "SELECT * FROM nom_vue",
      "SELECT * FROM temp.nom_vue",
      "SELECT * FROM global_temp.nom_vue",
      "SELECT * FROM global.nom_vue"
    ],
    correctIndex: 2,
    explanation: "Les Global Temp Views sont enregistrées dans un schéma spécial appelé 'global_temp'. Il faut donc y accéder avec le préfixe global_temp : SELECT * FROM global_temp.nom_vue."
  },
  {
    question: "Quelle est la portée d'une Temp View ?",
    options: [
      "Toute la base de données",
      "Tous les utilisateurs du cluster",
      "La session Spark courante uniquement",
      "Le notebook courant uniquement"
    ],
    correctIndex: 2,
    explanation: "Une Temp View (vue temporaire) n'existe que pendant la durée de la session Spark courante. Elle n'est visible que dans le notebook ou la connexion qui l'a créée."
  },
  {
    question: "Quel est l'avantage d'une CTE par rapport à une sous-requête ?",
    options: [
      "Les CTEs sont plus performantes",
      "Les CTEs sont stockées en mémoire",
      "Meilleure lisibilité et réutilisabilité dans la même requête",
      "Les CTEs peuvent être utilisées dans plusieurs requêtes"
    ],
    correctIndex: 2,
    explanation: "Les CTEs améliorent la lisibilité du code SQL en séparant les étapes logiques. Elles peuvent aussi être référencées plusieurs fois dans la même requête, évitant la duplication de sous-requêtes."
  },
  {
    question: "Les CTEs sont-elles stockées ?",
    options: [
      "Oui, dans le metastore",
      "Oui, en mémoire du cluster",
      "Non, elles n'existent que pendant l'exécution de la requête",
      "Oui, comme des vues temporaires"
    ],
    correctIndex: 2,
    explanation: "Les CTEs ne sont pas stockées. Elles n'existent que pendant l'exécution de la requête dans laquelle elles sont définies. Une fois la requête terminée, la CTE disparaît."
  }
];

const exercises: LessonExercise[] = [
  {
    id: "comparer-vues",
    title: "Comparer les 3 types de vues",
    description: "Créez chaque type de vue et testez leur persistance après redémarrage de la session.",
    difficulty: "facile",
    type: "code",
    prompt: "Créez une vue stockée, une vue temporaire et une vue globale temporaire à partir d'une table existante. Vérifiez leur accessibilité et testez ce qui se passe après un redémarrage de session.",
    hints: [
      "Utilisez CREATE VIEW pour la vue stockée, CREATE TEMP VIEW pour la temporaire, et CREATE GLOBAL TEMP VIEW pour la globale",
      "Accédez à la global temp view avec le préfixe global_temp",
      "Après un redémarrage, seule la vue stockée devrait être accessible"
    ],
    solution: {
      code: `-- Vue stockée (persistante)\nCREATE OR REPLACE VIEW vue_clients_actifs\nAS SELECT * FROM clients WHERE status = 'actif';\n\n-- Vue temporaire (session courante)\nCREATE OR REPLACE TEMP VIEW temp_clients\nAS SELECT * FROM clients WHERE pays = 'France';\n\n-- Vue globale temporaire (toutes les sessions du cluster)\nCREATE OR REPLACE GLOBAL TEMP VIEW global_clients_vip\nAS SELECT * FROM clients WHERE total_achats > 10000;\n\n-- Tester l'accès\nSELECT * FROM vue_clients_actifs;\nSELECT * FROM temp_clients;\nSELECT * FROM global_temp.global_clients_vip;`,
      language: "sql",
      explanation: "La vue stockée est persistée dans le metastore et accessible à tous. La temp view n'est visible que dans la session courante. La global temp view est accessible depuis toutes les sessions du cluster via le schéma global_temp, mais disparaît quand le cluster est redémarré."
    }
  },
  {
    id: "refactorer-ctes",
    title: "Refactorer avec des CTEs",
    description: "Réécrivez des sous-requêtes imbriquées en utilisant des CTEs pour améliorer la lisibilité.",
    difficulty: "moyen",
    type: "code",
    prompt: "Prenez une requête complexe avec des sous-requêtes imbriquées et réécrivez-la en utilisant des CTEs (WITH ... AS). La requête doit trouver les clients dont le total de commandes dépasse la moyenne.",
    hints: [
      "Commencez par identifier chaque sous-requête et donnez-lui un nom significatif",
      "Utilisez WITH cte_name AS (...) pour définir chaque étape",
      "Vous pouvez chaîner plusieurs CTEs avec des virgules"
    ],
    solution: {
      code: `-- Version avec sous-requêtes imbriquées (difficile à lire)\nSELECT * FROM (\n  SELECT client_id, SUM(amount) as total\n  FROM orders GROUP BY client_id\n) WHERE total > (\n  SELECT AVG(total) FROM (\n    SELECT SUM(amount) as total FROM orders GROUP BY client_id\n  )\n);\n\n-- Version refactorisée avec CTEs (lisible !)\nWITH totaux_clients AS (\n  SELECT client_id, SUM(amount) AS total\n  FROM orders\n  GROUP BY client_id\n),\nmoyenne_globale AS (\n  SELECT AVG(total) AS avg_total\n  FROM totaux_clients\n)\nSELECT tc.*\nFROM totaux_clients tc\nCROSS JOIN moyenne_globale mg\nWHERE tc.total > mg.avg_total;`,
      language: "sql",
      explanation: "Les CTEs permettent de décomposer la requête en étapes nommées et logiques. Ici, 'totaux_clients' calcule le total par client, et 'moyenne_globale' calcule la moyenne. La requête finale est beaucoup plus lisible et la CTE 'totaux_clients' est réutilisée."
    }
  }
];

export default function VuesCTEsPage() {
  return (
    <div className="flex min-h-[calc(100vh-4rem)]">
      <Sidebar currentPath="/modules/2-2-vues-ctes" />

      <main className="flex-1 overflow-y-auto">
        <div className="max-w-4xl mx-auto px-6 py-10 lg:px-10">
          {/* Header */}
          <div className="mb-10">
            <div className="flex items-center gap-3 mb-3">
              <span className="inline-flex items-center px-3 py-1 rounded-full text-xs font-semibold bg-blue-100 text-blue-800">
                Module 2
              </span>
              <span className="text-sm text-[var(--color-text-light)]">
                Leçon 2.2
              </span>
            </div>
            <h1 className="text-3xl font-bold text-[var(--color-text)] mb-3">
              Vues et CTEs
            </h1>
            <p className="text-lg text-[var(--color-text-light)] leading-relaxed">
              Maîtrisez les différents types de vues dans Databricks (stockées,
              temporaires, globales temporaires) et les expressions de table
              communes (CTEs) pour écrire des requêtes plus lisibles et
              maintenables.
            </p>
          </div>

          {/* Content */}
          <section className="space-y-8">
            {/* Introduction aux vues */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Qu&apos;est-ce qu&apos;une Vue ?
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Une <strong>vue</strong> est une requête SQL enregistrée qui
                agit comme une table virtuelle. Contrairement à une table, une
                vue ne stocke pas de données : elle exécute la requête
                sous-jacente à chaque fois qu&apos;elle est interrogée. Les vues
                sont utiles pour simplifier des requêtes complexes, restreindre
                l&apos;accès aux données, ou créer des abstractions logiques.
              </p>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Databricks supporte trois types de vues, chacun avec une portée
                et une persistance différentes.
              </p>
            </div>

            {/* Vue stockée */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                1. Vue Stockée (Persistante)
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Une vue stockée est <strong>persistée</strong> dans la base de
                données. Elle survit au redémarrage du cluster et de la session.
                Elle est accessible à tous les utilisateurs ayant accès à la
                base de données où elle est enregistrée.
              </p>
              <CodeBlock
                language="sql"
                title="Créer une vue stockée"
                code={`-- Créer une vue persistante
CREATE VIEW view_clients_actifs
AS SELECT id, name, email
FROM clients
WHERE status = 'actif';

-- Remplacer une vue existante
CREATE OR REPLACE VIEW view_clients_actifs
AS SELECT id, name, email, last_login
FROM clients
WHERE status = 'actif'
AND last_login > '2024-01-01';

-- Interroger la vue comme une table
SELECT * FROM view_clients_actifs;

-- Supprimer une vue
DROP VIEW IF EXISTS view_clients_actifs;`}
              />
            </div>

            {/* Vue temporaire */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                2. Vue Temporaire (Session)
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Une vue temporaire est liée à la{" "}
                <strong>session Spark</strong> en cours. Elle est{" "}
                <strong>automatiquement supprimée</strong> lorsque la session se
                termine (par exemple, lorsque le notebook est détaché du cluster
                ou que le cluster est redémarré).
              </p>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Les vues temporaires ne sont pas enregistrées dans le metastore
                et ne sont visibles que dans la session courante.
              </p>
              <CodeBlock
                language="sql"
                title="Créer une vue temporaire"
                code={`-- Créer une vue temporaire
CREATE TEMP VIEW temp_view_ventes
AS SELECT product_id, SUM(amount) AS total_ventes
FROM ventes
GROUP BY product_id;

-- Equivalent avec OR REPLACE
CREATE OR REPLACE TEMP VIEW temp_view_ventes
AS SELECT product_id, SUM(amount) AS total_ventes
FROM ventes
GROUP BY product_id;

-- Utiliser la vue temporaire
SELECT * FROM temp_view_ventes
WHERE total_ventes > 1000;`}
              />
            </div>

            {/* Vue globale temporaire */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                3. Vue Globale Temporaire (Cluster)
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Une vue globale temporaire a une portée au niveau du{" "}
                <strong>cluster</strong>. Elle est partagée entre toutes les
                sessions (notebooks) attachées au même cluster. Pour y accéder,
                vous devez utiliser le préfixe{" "}
                <code className="bg-gray-100 px-1.5 py-0.5 rounded text-sm font-mono">
                  global_temp
                </code>
                .
              </p>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                La vue globale temporaire est supprimée lorsque le cluster est
                arrêté ou redémarré.
              </p>
              <CodeBlock
                language="sql"
                title="Créer une vue globale temporaire"
                code={`-- Créer une vue globale temporaire
CREATE OR REPLACE GLOBAL TEMP VIEW global_view_stats
AS SELECT category, COUNT(*) AS count, AVG(price) AS avg_price
FROM products
GROUP BY category;

-- IMPORTANT : accéder via le préfixe global_temp
SELECT * FROM global_temp.global_view_stats;

-- Ceci ne fonctionnerait PAS :
-- SELECT * FROM global_view_stats; -- ERREUR !`}
              />
            </div>

            {/* Tableau comparatif */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Comparaison des types de vues
              </h2>
              <div className="overflow-x-auto mb-6">
                <table className="w-full border-collapse border border-gray-200 rounded-lg text-sm">
                  <thead>
                    <tr className="bg-gray-50">
                      <th className="border border-gray-200 px-4 py-3 text-left font-semibold text-[var(--color-text)]">
                        Caractéristique
                      </th>
                      <th className="border border-gray-200 px-4 py-3 text-left font-semibold text-[var(--color-text)]">
                        Vue Stockée
                      </th>
                      <th className="border border-gray-200 px-4 py-3 text-left font-semibold text-[var(--color-text)]">
                        Vue Temporaire
                      </th>
                      <th className="border border-gray-200 px-4 py-3 text-left font-semibold text-[var(--color-text)]">
                        Vue Globale Temporaire
                      </th>
                    </tr>
                  </thead>
                  <tbody className="text-[var(--color-text-light)]">
                    <tr>
                      <td className="border border-gray-200 px-4 py-3 font-medium">
                        Persistance
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Permanente (dans le metastore)
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Durée de la session
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Durée de vie du cluster
                      </td>
                    </tr>
                    <tr className="bg-gray-50/50">
                      <td className="border border-gray-200 px-4 py-3 font-medium">
                        Portée
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Base de données
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Session / Notebook
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Cluster (toutes sessions)
                      </td>
                    </tr>
                    <tr>
                      <td className="border border-gray-200 px-4 py-3 font-medium">
                        Accès
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        <code className="bg-gray-100 px-1 rounded text-xs font-mono">
                          SELECT * FROM view_name
                        </code>
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        <code className="bg-gray-100 px-1 rounded text-xs font-mono">
                          SELECT * FROM view_name
                        </code>
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        <code className="bg-gray-100 px-1 rounded text-xs font-mono">
                          SELECT * FROM global_temp.view_name
                        </code>
                      </td>
                    </tr>
                    <tr className="bg-gray-50/50">
                      <td className="border border-gray-200 px-4 py-3 font-medium">
                        Survit au redémarrage
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        ✅ Oui
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        ❌ Non
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        ❌ Non
                      </td>
                    </tr>
                  </tbody>
                </table>
              </div>

              <InfoBox type="tip" title="Quand utiliser chaque type ?">
                <ul className="list-disc list-inside space-y-1">
                  <li>
                    <strong>Vue stockée :</strong> pour des requêtes réutilisables
                    par toute l&apos;équipe, persistées entre les sessions.
                  </li>
                  <li>
                    <strong>Vue temporaire :</strong> pour des transformations
                    intermédiaires dans un même notebook, sans polluer le metastore.
                  </li>
                  <li>
                    <strong>Vue globale temporaire :</strong> pour partager des
                    résultats entre notebooks sur un même cluster, sans les
                    persister.
                  </li>
                </ul>
              </InfoBox>
            </div>

            {/* CTEs */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                CTEs — Common Table Expressions
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Les <strong>CTEs</strong> (expressions de table communes)
                utilisent la clause <strong>WITH</strong> pour définir des
                résultats temporaires nommés au sein d&apos;une seule requête.
                Elles rendent les requêtes complexes plus lisibles et plus
                faciles à maintenir, en comparaison aux sous-requêtes imbriquées.
              </p>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Les CTEs n&apos;existent que pendant l&apos;exécution de la
                requête : elles ne sont pas enregistrées dans le metastore et
                ne persistent pas au-delà de la requête.
              </p>
              <CodeBlock
                language="sql"
                title="Utiliser des CTEs"
                code={`-- CTE simple
WITH ventes_par_produit AS (
  SELECT product_id, SUM(amount) AS total
  FROM ventes
  GROUP BY product_id
)
SELECT p.name, v.total
FROM ventes_par_produit v
JOIN products p ON v.product_id = p.id
WHERE v.total > 5000;

-- CTEs multiples (chaînées)
WITH
  ventes_mensuelles AS (
    SELECT product_id, MONTH(date) AS mois, SUM(amount) AS total
    FROM ventes
    GROUP BY product_id, MONTH(date)
  ),
  top_produits AS (
    SELECT product_id, SUM(total) AS total_annuel
    FROM ventes_mensuelles
    GROUP BY product_id
    ORDER BY total_annuel DESC
    LIMIT 10
  )
SELECT p.name, t.total_annuel
FROM top_produits t
JOIN products p ON t.product_id = p.id;`}
              />

              <InfoBox type="info" title="CTEs vs Sous-requêtes">
                <p>
                  Les CTEs et les sous-requêtes produisent le même résultat, mais
                  les CTEs sont généralement <strong>plus lisibles</strong>,
                  surtout pour les requêtes complexes avec plusieurs niveaux
                  d&apos;imbrication. De plus, une CTE peut être{" "}
                  <strong>référencée plusieurs fois</strong> dans la même
                  requête, ce qui évite la duplication de code. Privilégiez les
                  CTEs pour la clarté de votre code SQL.
                </p>
              </InfoBox>
            </div>
          </section>

          {/* Quiz */}
          <Quiz
            lessonSlug="2-2-vues-ctes"
            title="Quiz — Vues et CTEs"
            questions={quizQuestions}
          />

          {/* Exercices */}
          <LessonExercises
            lessonSlug="2-2-vues-ctes"
            exercises={exercises}
          />

          {/* Bouton de complétion */}
          <LessonCompleteButton lessonSlug="2-2-vues-ctes" />

          {/* Navigation */}
          <div className="flex items-center justify-between mt-12 pt-6 border-t border-gray-200">
            <Link
              href="/modules/2-1-bases-donnees-tables"
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
              Leçon précédente : Bases de données et Tables
            </Link>
            <Link
              href="/modules/2-3-transformations-donnees"
              className="inline-flex items-center gap-2 px-5 py-2.5 bg-[#ff3621] text-white rounded-lg font-medium hover:bg-[#e02e1a] transition-colors"
            >
              Leçon suivante : Transformations de Données
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
