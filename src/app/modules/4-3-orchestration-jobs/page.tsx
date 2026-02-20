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
    question: "Quel est l'avantage principal des Job Clusters par rapport aux All-Purpose Clusters ?",
    options: ["Plus puissants", "Créés et détruits automatiquement, donc moins coûteux", "Plus faciles à configurer", "Compatibles avec plus de langages"],
    correctIndex: 1,
    explanation: "Les Job Clusters sont optimisés pour les coûts : créés au début du job et détruits à la fin.",
  },
  {
    question: "Comment passer des données entre deux tâches d'un job ?",
    options: ["Via des variables globales", "Avec dbutils.jobs.taskValues.set() et .get()", "Par fichiers temporaires uniquement", "Ce n'est pas possible"],
    correctIndex: 1,
    explanation: "dbutils.jobs.taskValues permet de passer des valeurs entre tâches sans fichiers intermédiaires.",
  },
  {
    question: "Que fait la fonctionnalité 'Repair Run' d'un job ?",
    options: ["Répare les données corrompues", "Ré-exécute uniquement les tâches qui ont échoué", "Supprime et recrée le job", "Restaure une version précédente"],
    correctIndex: 1,
    explanation: "Repair Run est très utile car il ne relance que les tâches en échec, économisant du temps et des ressources.",
  },
  {
    question: "Quels types de déclencheurs un job Databricks supporte-t-il ?",
    options: ["Uniquement manuel", "Manuel et planifié (cron)", "Manuel, planifié (cron) et arrivée de fichier", "Uniquement API"],
    correctIndex: 2,
    explanation: "Databricks supporte trois types : manuel, cron (planifié), et file arrival trigger (déclenchement à l'arrivée de fichiers).",
  },
  {
    question: "Les tâches d'un job peuvent-elles s'exécuter en parallèle ?",
    options: ["Non, toujours séquentielles", "Oui, si elles n'ont pas de dépendances entre elles", "Oui, toujours en parallèle", "Seulement avec un cluster Multi-Node"],
    correctIndex: 1,
    explanation: "Les tâches sans dépendances directes peuvent s'exécuter en parallèle, accélérant le temps total du job.",
  },
];

const exercises: LessonExercise[] = [
  {
    id: "concevoir-workflow",
    title: "Concevoir un workflow multi-tâches",
    description: "Concevez un workflow ETL complet avec 5 tâches et des dépendances entre elles.",
    difficulty: "moyen",
    type: "reflexion",
    prompt: "Concevez un workflow Databricks avec 5 tâches pour un pipeline ETL e-commerce : ingestion des commandes, ingestion des clients, jointure commandes-clients, calcul des métriques, et génération d'un rapport. Définissez les dépendances entre les tâches et identifiez lesquelles peuvent s'exécuter en parallèle.",
    hints: [
      "Les deux tâches d'ingestion sont indépendantes et peuvent s'exécuter en parallèle",
      "La jointure dépend des deux ingestions",
      "Pensez à utiliser des Job Clusters pour réduire les coûts",
    ],
    solution: {
      explanation: "Workflow optimal : Tâche 1 (Ingestion commandes) et Tâche 2 (Ingestion clients) s'exécutent en parallèle car indépendantes. Tâche 3 (Jointure) dépend de Tâche 1 ET Tâche 2. Tâche 4 (Métriques) dépend de Tâche 3. Tâche 5 (Rapport) dépend de Tâche 4. Le DAG forme un losange puis une séquence : T1 et T2 convergent vers T3, puis T4, puis T5. Temps total réduit car T1 et T2 sont parallèles.",
    },
  },
  {
    id: "implementer-task-value",
    title: "Implémenter un task value",
    description: "Écrivez le code Python pour passer des valeurs entre tâches d'un job.",
    difficulty: "moyen",
    type: "code",
    prompt: "Écrivez le code Python pour deux tâches d'un job Databricks : la tâche 'extract' qui compte les lignes d'un DataFrame et transmet ce nombre via Task Values, et la tâche 'report' qui récupère cette valeur et affiche un message de résumé.",
    hints: [
      "Utilisez dbutils.jobs.taskValues.set(key, value) pour émettre",
      "Utilisez dbutils.jobs.taskValues.get(taskKey, key) pour recevoir",
      "Le taskKey correspond au nom de la tâche source",
    ],
    solution: {
      code: `# === Tâche "extract" ===\n# Simuler un traitement de données\ndf = spark.read.table("catalog.schema.orders")\nrow_count = df.count()\n\n# Transmettre le nombre de lignes à la tâche suivante\ndbutils.jobs.taskValues.set(key="rows_processed", value=row_count)\ndbutils.jobs.taskValues.set(key="status", value="success")\nprint(f"Extract terminé : {row_count} lignes")\n\n# === Tâche "report" ===\n# Récupérer les valeurs de la tâche extract\nrows = dbutils.jobs.taskValues.get(\n    taskKey="extract",\n    key="rows_processed"\n)\nstatus = dbutils.jobs.taskValues.get(\n    taskKey="extract",\n    key="status"\n)\nprint(f"Rapport : {rows} lignes traitées, statut = {status}")`,
      language: "python",
      explanation: "dbutils.jobs.taskValues.set() stocke une valeur identifiée par une clé. dbutils.jobs.taskValues.get() récupère la valeur en spécifiant le nom de la tâche source (taskKey) et la clé. C'est le mécanisme recommandé pour les données légères entre tâches.",
    },
  },
];

export default function OrchestrationJobsPage() {
  return (
    <div className="flex min-h-[calc(100vh-4rem)]">
      <Sidebar currentPath="/modules/4-3-orchestration-jobs" />

      <main className="flex-1 overflow-y-auto">
        <div className="max-w-4xl mx-auto px-6 py-10 lg:px-10">
          {/* Header */}
          <div className="mb-10">
            <div className="flex items-center gap-3 mb-3">
              <span className="inline-flex items-center px-3 py-1 rounded-full text-xs font-semibold bg-blue-100 text-blue-800">
                Module 4
              </span>
              <span className="text-sm text-[var(--color-text-light)]">
                Leçon 4.3
              </span>
            </div>
            <h1 className="text-3xl font-bold text-[var(--color-text)] mb-3">
              Orchestration avec Jobs
            </h1>
            <p className="text-lg text-[var(--color-text-light)] leading-relaxed">
              Maîtrisez Databricks Workflows pour orchestrer vos pipelines de
              données en production. Apprenez à créer des jobs multi-tâches,
              configurer les dépendances, planifier les exécutions et
              optimiser les coûts avec les Job Clusters.
            </p>
          </div>

          {/* Content */}
          <section className="space-y-8">
            {/* Introduction */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Qu&apos;est-ce que Databricks Workflows ?
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                <strong>Databricks Workflows</strong> (anciennement appelé
                &quot;Jobs&quot;) est le service d&apos;orchestration natif de
                Databricks. Il permet de planifier et exécuter des tâches de
                manière automatisée, avec un support complet pour les
                dépendances entre tâches, les notifications, et la reprise
                sur erreur.
              </p>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Un <strong>Job</strong> est un ensemble de tâches organisées
                en un graphe de dépendances (DAG). Chaque tâche peut être un
                notebook, un pipeline DLT, une requête SQL, un script Python,
                un fichier JAR ou une commande Spark Submit.
              </p>
            </div>

            {/* Task Types */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Types de tâches supportés
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Databricks Workflows supporte plusieurs types de tâches que
                vous pouvez combiner dans un même job :
              </p>
              <div className="overflow-x-auto mb-4">
                <table className="w-full border-collapse border border-gray-300 text-sm">
                  <thead>
                    <tr className="bg-gray-100">
                      <th className="border border-gray-300 px-4 py-2 text-left font-semibold text-[var(--color-text)]">
                        Type de tâche
                      </th>
                      <th className="border border-gray-300 px-4 py-2 text-left font-semibold text-[var(--color-text)]">
                        Description
                      </th>
                      <th className="border border-gray-300 px-4 py-2 text-left font-semibold text-[var(--color-text)]">
                        Cas d&apos;utilisation
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr>
                      <td className="border border-gray-300 px-4 py-2 font-semibold text-[var(--color-text-light)]">
                        Notebook
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Exécute un notebook Databricks
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Transformations, analyses, ML
                      </td>
                    </tr>
                    <tr>
                      <td className="border border-gray-300 px-4 py-2 font-semibold text-[var(--color-text-light)]">
                        Pipeline DLT
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Déclenche un pipeline Delta Live Tables
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Pipelines de données déclaratifs
                      </td>
                    </tr>
                    <tr>
                      <td className="border border-gray-300 px-4 py-2 font-semibold text-[var(--color-text-light)]">
                        Requête SQL
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Exécute une requête SQL dans un SQL Warehouse
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Rapports, agrégations, maintenance
                      </td>
                    </tr>
                    <tr>
                      <td className="border border-gray-300 px-4 py-2 font-semibold text-[var(--color-text-light)]">
                        Python Script
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Exécute un fichier Python
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Scripts utilitaires, intégrations
                      </td>
                    </tr>
                    <tr>
                      <td className="border border-gray-300 px-4 py-2 font-semibold text-[var(--color-text-light)]">
                        JAR
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Exécute une application Java/Scala packagée
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Applications Spark complexes
                      </td>
                    </tr>
                    <tr>
                      <td className="border border-gray-300 px-4 py-2 font-semibold text-[var(--color-text-light)]">
                        Spark Submit
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Soumet une application Spark
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Migration de jobs Spark existants
                      </td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </div>

            {/* Creating a Multi-Task Job */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Créer un job multi-tâches
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Voici les étapes pour créer un job orchestrant plusieurs
                tâches dans Databricks :
              </p>
              <ol className="list-decimal list-inside space-y-4 text-[var(--color-text-light)] mb-4">
                <li>
                  <strong>Accéder à Workflows</strong> : dans la barre
                  latérale de Databricks, cliquez sur{" "}
                  <strong>Workflows</strong>.
                </li>
                <li>
                  <strong>Créer un Job</strong> : cliquez sur{" "}
                  <strong>Create Job</strong> et donnez-lui un nom descriptif.
                </li>
                <li>
                  <strong>Ajouter des tâches</strong> : ajoutez chaque tâche
                  en spécifiant son type (Notebook, DLT, SQL, etc.), sa
                  source, et ses <strong>dépendances</strong> vis-à-vis
                  d&apos;autres tâches.
                </li>
                <li>
                  <strong>Configurer la planification</strong> : définissez
                  un déclencheur : manuel, planifié (expression cron), ou
                  déclenché par l&apos;arrivée de fichiers.
                </li>
                <li>
                  <strong>Configurer les alertes</strong> : définissez des
                  notifications par email en cas de succès, d&apos;échec ou
                  de timeout.
                </li>
              </ol>
            </div>

            {/* Task Dependencies */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Dépendances entre tâches
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Les tâches d&apos;un job peuvent être exécutées de manière{" "}
                <strong>séquentielle</strong> ou <strong>parallèle</strong>{" "}
                en fonction des dépendances définies :
              </p>
              <ul className="list-disc list-inside space-y-3 text-[var(--color-text-light)] mb-4">
                <li>
                  <strong>Exécution séquentielle</strong> : une tâche attend
                  la fin de la tâche précédente avant de démarrer. Utile
                  quand une tâche dépend du résultat de la précédente.
                </li>
                <li>
                  <strong>Exécution parallèle</strong> : plusieurs tâches
                  sans dépendances mutuelles s&apos;exécutent simultanément.
                  Cela réduit le temps total d&apos;exécution du job.
                </li>
              </ul>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Le DAG du job est automatiquement généré par Databricks en
                fonction des dépendances que vous définissez. Il offre une
                vue claire de l&apos;ordre d&apos;exécution et des
                parallélismes possibles.
              </p>
            </div>

            {/* Scheduling */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Planification des jobs
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Databricks offre trois modes de déclenchement :
              </p>
              <ul className="list-disc list-inside space-y-3 text-[var(--color-text-light)] mb-4">
                <li>
                  <strong>Manuel</strong> : le job est déclenché à la demande
                  via l&apos;interface ou l&apos;API.
                </li>
                <li>
                  <strong>Planifié (Cron)</strong> : le job s&apos;exécute
                  automatiquement selon une expression cron (ex :{" "}
                  <code>0 0 * * *</code> pour tous les jours à minuit).
                </li>
                <li>
                  <strong>File Arrival Trigger</strong> : le job se déclenche
                  automatiquement lorsqu&apos;un nouveau fichier arrive dans
                  un emplacement de stockage cloud spécifié.
                </li>
              </ul>
            </div>

            {/* Job Clusters vs All-Purpose Clusters */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Job Clusters vs All-Purpose Clusters
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Le choix du type de cluster pour vos jobs a un impact
                significatif sur les coûts :
              </p>
              <div className="overflow-x-auto mb-4">
                <table className="w-full border-collapse border border-gray-300 text-sm">
                  <thead>
                    <tr className="bg-gray-100">
                      <th className="border border-gray-300 px-4 py-2 text-left font-semibold text-[var(--color-text)]">
                        Caractéristique
                      </th>
                      <th className="border border-gray-300 px-4 py-2 text-left font-semibold text-[var(--color-text)]">
                        Job Cluster
                      </th>
                      <th className="border border-gray-300 px-4 py-2 text-left font-semibold text-[var(--color-text)]">
                        All-Purpose Cluster
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Coût
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Tarif réduit (Jobs Compute)
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Tarif standard (All-Purpose Compute)
                      </td>
                    </tr>
                    <tr>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Cycle de vie
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Créé au lancement, détruit à la fin du job
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Persiste entre les exécutions
                      </td>
                    </tr>
                    <tr>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Cas d&apos;utilisation
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Jobs planifiés en production
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Développement interactif, exploration
                      </td>
                    </tr>
                    <tr>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Accès interactif
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Non (exécution automatisée uniquement)
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Oui (notebooks, terminal, etc.)
                      </td>
                    </tr>
                    <tr>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Partage
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Dédié à un seul job
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Partageable entre utilisateurs
                      </td>
                    </tr>
                  </tbody>
                </table>
              </div>

              <InfoBox type="important" title="Utilisez les Job Clusters pour réduire les coûts">
                <p>
                  En production, utilisez toujours des{" "}
                  <strong>Job Clusters</strong> plutôt que des All-Purpose
                  Clusters pour vos jobs planifiés. Les Job Clusters offrent
                  un tarif significativement réduit et sont automatiquement
                  créés et détruits à chaque exécution, évitant ainsi les
                  coûts de clusters inactifs.
                </p>
              </InfoBox>
            </div>

            {/* Task Values */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Passage de paramètres entre tâches
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Databricks permet de passer des valeurs entre les tâches
                d&apos;un même job grâce au mécanisme de{" "}
                <strong>Task Values</strong>. Une tâche en amont peut
                définir une valeur qui sera lue par une tâche en aval :
              </p>
              <CodeBlock
                language="python"
                title="Passage de valeurs entre tâches avec dbutils"
                code={`# === Dans la tâche en amont (tâche "extract") ===

# Définir une valeur de tâche
dbutils.jobs.taskValues.set(key="my_key", value="my_value")

# Exemple concret : transmettre le nombre de lignes traitées
row_count = df.count()
dbutils.jobs.taskValues.set(key="rows_processed", value=row_count)

# On peut aussi transmettre des structures plus complexes
dbutils.jobs.taskValues.set(
    key="metadata",
    value={"table": "orders", "rows": row_count, "status": "success"}
)`}
              />
              <CodeBlock
                language="python"
                title="Récupérer les valeurs dans une tâche en aval"
                code={`# === Dans la tâche en aval (tâche "transform") ===

# Récupérer la valeur définie par la tâche en amont
value = dbutils.jobs.taskValues.get(
    taskKey="extract",    # nom de la tâche en amont
    key="my_key"          # clé de la valeur
)

# Récupérer le nombre de lignes traitées
rows = dbutils.jobs.taskValues.get(
    taskKey="extract",
    key="rows_processed"
)
print(f"La tâche extract a traité {rows} lignes")

# Récupérer les métadonnées
metadata = dbutils.jobs.taskValues.get(
    taskKey="extract",
    key="metadata"
)
print(f"Table: {metadata['table']}, Lignes: {metadata['rows']}")`}
              />

              <InfoBox type="tip" title="Task Values : communication entre tâches">
                <p>
                  Les <strong>Task Values</strong> sont le mécanisme
                  recommandé pour passer des données légères entre les tâches
                  d&apos;un job (compteurs, statuts, chemins de fichiers).
                  Pour des volumes de données plus importants, écrivez les
                  données dans une table Delta et lisez-les depuis la tâche
                  en aval.
                </p>
              </InfoBox>
            </div>

            {/* Repair and Re-run */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Réparation et ré-exécution des tâches échouées
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Lorsqu&apos;une tâche échoue dans un job, vous n&apos;avez
                pas besoin de relancer l&apos;ensemble du job. La
                fonctionnalité <strong>Repair Run</strong> permet de
                ré-exécuter uniquement les tâches échouées et leurs
                dépendances en aval, en conservant les résultats des tâches
                qui ont réussi.
              </p>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Cela permet de gagner du temps et de réduire les coûts en
                évitant de recalculer des résultats déjà valides.
              </p>

              <InfoBox type="info" title="Repair Run : ré-exécution intelligente">
                <p>
                  La fonctionnalité <strong>Repair Run</strong> est
                  particulièrement utile pour les jobs longs avec de
                  nombreuses tâches. Si une seule tâche échoue (par exemple
                  à cause d&apos;un problème réseau transitoire), vous pouvez
                  réparer uniquement cette tâche sans relancer les tâches
                  précédentes qui ont réussi.
                </p>
              </InfoBox>
            </div>

            {/* Retry and Timeout */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Politiques de retry et timeout
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Chaque tâche peut être configurée avec des politiques de
                reprise et de timeout :
              </p>
              <ul className="list-disc list-inside space-y-3 text-[var(--color-text-light)] mb-4">
                <li>
                  <strong>Retry Policy</strong> : définit le nombre maximum
                  de tentatives en cas d&apos;échec. Vous pouvez configurer
                  le nombre de retries et le délai entre chaque tentative.
                </li>
                <li>
                  <strong>Timeout</strong> : durée maximale d&apos;exécution
                  d&apos;une tâche. Si la tâche dépasse ce temps, elle est
                  automatiquement arrêtée et marquée comme échouée.
                </li>
              </ul>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Ces paramètres sont configurables dans l&apos;interface
                Databricks au niveau de chaque tâche individuelle.
              </p>
            </div>

            {/* Notifications */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Notifications par email
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Databricks Workflows permet de configurer des notifications
                automatiques par email pour différents événements :
              </p>
              <ul className="list-disc list-inside space-y-2 text-[var(--color-text-light)] mb-4">
                <li>
                  <strong>On Start</strong> : notification au démarrage du job
                </li>
                <li>
                  <strong>On Success</strong> : notification en cas de succès
                </li>
                <li>
                  <strong>On Failure</strong> : notification en cas
                  d&apos;échec
                </li>
                <li>
                  <strong>On Duration Warning</strong> : notification si le
                  job dépasse une durée seuil
                </li>
              </ul>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Ces notifications sont essentielles pour la surveillance des
                jobs en production et permettent de réagir rapidement en cas
                de problème.
              </p>
            </div>

            {/* Summary */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Résumé : bonnes pratiques d&apos;orchestration
              </h2>
              <ul className="list-disc list-inside space-y-3 text-[var(--color-text-light)]">
                <li>
                  Utilisez des <strong>Job Clusters</strong> pour les jobs
                  planifiés (économies de coûts).
                </li>
                <li>
                  Définissez les dépendances entre tâches pour maximiser le{" "}
                  <strong>parallélisme</strong>.
                </li>
                <li>
                  Configurez des <strong>retry policies</strong> pour gérer
                  les erreurs transitoires.
                </li>
                <li>
                  Utilisez <strong>Task Values</strong> pour passer des
                  paramètres entre tâches.
                </li>
                <li>
                  Exploitez <strong>Repair Run</strong> pour ré-exécuter
                  uniquement les tâches échouées.
                </li>
                <li>
                  Configurez les <strong>notifications email</strong> pour
                  surveiller vos jobs en production.
                </li>
                <li>
                  Utilisez le déclencheur{" "}
                  <strong>File Arrival Trigger</strong> quand l&apos;arrivée
                  de fichiers doit initier un traitement.
                </li>
              </ul>
            </div>
          </section>

          {/* Quiz */}
          <Quiz
            lessonSlug="4-3-orchestration-jobs"
            title="Quiz — Orchestration avec Jobs"
            questions={quizQuestions}
          />

          {/* Exercices */}
          <LessonExercises
            lessonSlug="4-3-orchestration-jobs"
            exercises={exercises}
          />

          {/* Complétion */}
          <LessonCompleteButton lessonSlug="4-3-orchestration-jobs" />

          {/* Navigation */}
          <div className="flex flex-col sm:flex-row justify-between gap-4 mt-12 pt-8 border-t border-[var(--color-border)]">
            <Link
              href="/modules/4-2-resultats-pipeline"
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
              Leçon précédente : Résultats du Pipeline
            </Link>
            <Link
              href="/modules/5-1-unity-catalog"
              className="inline-flex items-center gap-2 px-5 py-2.5 bg-[#ff3621] text-white rounded-lg font-medium hover:bg-[#e02e1a] transition-colors"
            >
              Leçon suivante : Unity Catalog
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
