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
    question: "Quel est le rôle du checkpoint dans Structured Streaming ?",
    options: [
      "Accélérer le traitement",
      "Assurer la tolérance aux pannes en sauvegardant l'état du stream",
      "Compresser les données",
      "Supprimer les doublons"
    ],
    correctIndex: 1,
    explanation: "Le checkpoint sauvegarde l'état du stream (offsets lus, état des agrégations) sur un stockage fiable. En cas de panne, le stream peut reprendre exactement là où il s'est arrêté, garantissant un traitement exactly-once."
  },
  {
    question: "Quel trigger est recommandé pour remplacer trigger once ?",
    options: [
      "trigger(once=True)",
      "trigger(processingTime='1 minute')",
      "trigger(availableNow=True)",
      "trigger(continuous=True)"
    ],
    correctIndex: 2,
    explanation: "trigger(availableNow=True) est le remplacement recommandé de trigger(once=True). Il traite toutes les données disponibles en plusieurs micro-batches (meilleure scalabilité) puis s'arrête, contrairement à once qui force tout dans un seul batch."
  },
  {
    question: "Quel output mode utiliser pour un compteur incrémental (ex: COUNT par catégorie) ?",
    options: [
      "append",
      "complete",
      "update",
      "overwrite"
    ],
    correctIndex: 1,
    explanation: "Le mode 'complete' réécrit la totalité de la table de résultats à chaque micro-batch. C'est nécessaire pour les agrégations comme COUNT car le résultat peut changer pour des clés existantes. Le mode 'append' ne fonctionne pas avec les agrégations sans watermark."
  },
  {
    question: "Que fait spark.readStream par rapport à spark.read ?",
    options: [
      "C'est identique",
      "readStream lit les données de manière incrémentale au fil du temps",
      "readStream est plus rapide",
      "readStream ne supporte que le JSON"
    ],
    correctIndex: 1,
    explanation: "spark.readStream crée un DataFrame en streaming qui lit les nouvelles données de manière incrémentale au fur et à mesure qu'elles arrivent. Contrairement à spark.read qui lit un snapshot statique, readStream surveille en continu la source pour de nouvelles données."
  },
  {
    question: "Peut-on faire un ORDER BY sur un stream ?",
    options: [
      "Oui sans restriction",
      "Oui mais seulement après un groupBy",
      "Non, le tri n'est pas supporté sur les données en streaming",
      "Oui avec un watermark"
    ],
    correctIndex: 2,
    explanation: "Le tri (ORDER BY) n'est pas supporté sur les données en streaming car il nécessiterait de connaître toutes les données, ce qui est impossible avec un flux potentiellement infini. Seul le tri après un groupBy en mode complete est techniquement possible."
  }
];

const exercises: LessonExercise[] = [
  {
    id: "creer-stream-simple",
    title: "Créer un stream simple",
    description: "Écrivez le code complet pour créer un pipeline de streaming : lire un stream, créer une vue temporaire, puis écrire le résultat avec checkpointing.",
    difficulty: "moyen",
    type: "code",
    prompt: "Créez un pipeline Structured Streaming qui lit des fichiers JSON depuis '/mnt/data/events', crée une vue temporaire 'events_stream', puis écrit les résultats en Delta avec un checkpoint.",
    hints: [
      "Utilisez spark.readStream.format('json') pour lire le flux",
      "createOrReplaceTempView permet de créer une vue temporaire sur un DataFrame en streaming",
      "writeStream nécessite un checkpointLocation et un outputMode"
    ],
    solution: {
      code: `# 1. Lire le stream\ndf_stream = (spark.readStream\n  .format("json")\n  .schema("event_id STRING, event_type STRING, timestamp TIMESTAMP, user_id STRING")\n  .load("/mnt/data/events")\n)\n\n# 2. Créer une vue temporaire\ndf_stream.createOrReplaceTempView("events_stream")\n\n# 3. Optionnel : requête SQL sur la vue\nresult = spark.sql("SELECT event_type, COUNT(*) as count FROM events_stream GROUP BY event_type")\n\n# 4. Écrire le résultat en Delta\nquery = (result.writeStream\n  .format("delta")\n  .outputMode("complete")\n  .option("checkpointLocation", "/mnt/checkpoints/events")\n  .trigger(availableNow=True)\n  .toTable("events_summary")\n)`,
      language: "python",
      explanation: "Ce pipeline lit des fichiers JSON en streaming, crée une vue temporaire pour permettre des requêtes SQL, puis écrit les agrégations en mode complete dans une table Delta avec checkpointing pour la tolérance aux pannes."
    }
  },
  {
    id: "comparer-triggers",
    title: "Comparer les triggers",
    description: "Expliquez les différences entre les différents types de triggers disponibles dans Structured Streaming.",
    difficulty: "moyen",
    type: "reflexion",
    prompt: "Décrivez les différences entre trigger(availableNow=True), trigger(processingTime='1 minute'), et le trigger once (déprécié). Dans quels cas utiliser chacun ?",
    hints: [
      "Pensez au mode de traitement : continu vs batch",
      "Considérez la scalabilité : un seul batch vs plusieurs micro-batches",
      "Réfléchissez aux cas d'usage : temps réel vs batch planifié"
    ],
    solution: {
      code: `# trigger(processingTime='1 minute')\n# → Traitement continu avec des micro-batches toutes les minutes\n# → Cas d'usage : dashboards temps réel, monitoring\n# → Le stream tourne en permanence\n\n# trigger(once=True) — DÉPRÉCIÉ\n# → Traite TOUTES les données disponibles en UN SEUL batch\n# → Problème : peut manquer de mémoire avec beaucoup de données\n# → Remplacé par availableNow\n\n# trigger(availableNow=True) — RECOMMANDÉ\n# → Traite toutes les données disponibles en PLUSIEURS micro-batches\n# → Meilleure scalabilité que once\n# → S'arrête une fois toutes les données traitées\n# → Cas d'usage : pipelines batch planifiés (ex: toutes les heures via un job)`,
      language: "python",
      explanation: "La principale distinction est entre le traitement continu (processingTime) et le traitement batch (availableNow). Le trigger availableNow remplace avantageusement once car il divise le travail en plusieurs micro-batches, évitant les problèmes de mémoire avec de grands volumes de données."
    }
  }
];

export default function StructuredStreamingPage() {
  return (
    <div className="flex min-h-[calc(100vh-4rem)]">
      <Sidebar currentPath="/modules/3-1-structured-streaming" />

      <main className="flex-1 overflow-y-auto">
        <div className="max-w-4xl mx-auto px-6 py-10 lg:px-10">
          {/* Header */}
          <div className="mb-10">
            <div className="flex items-center gap-3 mb-3">
              <span className="inline-flex items-center px-3 py-1 rounded-full text-xs font-semibold bg-blue-100 text-blue-800">
                Module 3
              </span>
              <span className="text-sm text-[var(--color-text-light)]">
                Leçon 3.1
              </span>
            </div>
            <h1 className="text-3xl font-bold text-[var(--color-text)] mb-3">
              Structured Streaming
            </h1>
            <p className="text-lg text-[var(--color-text-light)] leading-relaxed">
              Découvrez Spark Structured Streaming, le moteur de traitement de
              flux en temps réel intégré à Apache Spark. Apprenez à lire et
              écrire des flux de données, à configurer les déclencheurs et à
              garantir la tolérance aux pannes grâce aux checkpoints.
            </p>
          </div>

          {/* Content */}
          <section className="space-y-8">
            {/* Introduction */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Qu&apos;est-ce que Structured Streaming ?
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                <strong>Spark Structured Streaming</strong> est un moteur de
                traitement de flux construit sur le moteur Spark SQL. Il permet
                de traiter des données en continu (streaming) en utilisant la
                même API que pour le traitement par lots (batch). L&apos;idée
                centrale est de considérer un flux de données comme une{" "}
                <strong>table non bornée</strong> (unbounded table) qui
                s&apos;enrichit continuellement de nouvelles lignes.
              </p>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Cette approche simplifie considérablement le développement :
                vous écrivez votre logique de transformation une seule fois, et
                Spark se charge de l&apos;appliquer de manière incrémentale à
                mesure que de nouvelles données arrivent.
              </p>
            </div>

            {/* Streaming vs Batch */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Streaming vs Batch
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                En traitement <strong>batch</strong>, les données sont traitées
                en blocs finis : on lit un ensemble complet de données, on les
                transforme, puis on écrit le résultat. En{" "}
                <strong>streaming</strong>, les données arrivent en continu et
                sont traitées au fur et à mesure de leur arrivée.
              </p>
              <div className="overflow-x-auto my-6">
                <table className="w-full border-collapse border border-gray-200 text-sm">
                  <thead>
                    <tr className="bg-gray-50">
                      <th className="border border-gray-200 px-4 py-3 text-left font-semibold text-[var(--color-text)]">
                        Critère
                      </th>
                      <th className="border border-gray-200 px-4 py-3 text-left font-semibold text-[var(--color-text)]">
                        Batch
                      </th>
                      <th className="border border-gray-200 px-4 py-3 text-left font-semibold text-[var(--color-text)]">
                        Streaming
                      </th>
                    </tr>
                  </thead>
                  <tbody className="text-[var(--color-text-light)]">
                    <tr>
                      <td className="border border-gray-200 px-4 py-3 font-medium">
                        Données
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Ensemble fini, borné
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Flux continu, non borné
                      </td>
                    </tr>
                    <tr className="bg-gray-50">
                      <td className="border border-gray-200 px-4 py-3 font-medium">
                        Latence
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Minutes à heures
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Secondes à minutes
                      </td>
                    </tr>
                    <tr>
                      <td className="border border-gray-200 px-4 py-3 font-medium">
                        Exécution
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Ponctuelle ou planifiée
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Continue ou déclenchée
                      </td>
                    </tr>
                    <tr className="bg-gray-50">
                      <td className="border border-gray-200 px-4 py-3 font-medium">
                        API Spark
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        spark.read / spark.write
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        spark.readStream / spark.writeStream
                      </td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </div>

            {/* Key concepts */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Concepts clés
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Structured Streaming repose sur trois concepts fondamentaux :
              </p>
              <ul className="list-disc list-inside space-y-3 text-[var(--color-text-light)] mb-4">
                <li>
                  <strong>Input Table (Table d&apos;entrée)</strong> : La source
                  de données streaming est traitée comme une table qui grandit
                  continuellement. Chaque nouveau lot de données est ajouté
                  comme de nouvelles lignes à cette table.
                </li>
                <li>
                  <strong>Result Table (Table de résultat)</strong> : Le
                  résultat de la requête appliquée sur la table d&apos;entrée.
                  Elle est mise à jour de manière incrémentale à chaque micro-batch.
                </li>
                <li>
                  <strong>Output Modes (Modes de sortie)</strong> : Définissent
                  ce qui est écrit dans le sink (destination) à chaque
                  déclenchement.
                </li>
              </ul>
            </div>

            {/* Output modes */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Modes de sortie (Output Modes)
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Il existe trois modes de sortie qui déterminent comment les
                résultats sont écrits dans la table de destination :
              </p>
              <ul className="list-disc list-inside space-y-3 text-[var(--color-text-light)] mb-4">
                <li>
                  <strong>Append</strong> (par défaut) : Seules les nouvelles
                  lignes ajoutées à la Result Table depuis le dernier
                  déclenchement sont écrites. Idéal pour les requêtes sans
                  agrégation.
                </li>
                <li>
                  <strong>Complete</strong> : L&apos;intégralité de la Result
                  Table est réécrite à chaque déclenchement. Utilisé avec les
                  agrégations (GROUP BY).
                </li>
                <li>
                  <strong>Update</strong> : Seules les lignes modifiées depuis
                  le dernier déclenchement sont écrites. Plus efficace que
                  Complete pour les agrégations.
                </li>
              </ul>
            </div>

            {/* Reading a stream */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Lire un flux avec spark.readStream
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Pour lire des données en streaming, on utilise{" "}
                <code className="bg-gray-100 px-1.5 py-0.5 rounded text-sm font-mono">
                  spark.readStream
                </code>{" "}
                au lieu de{" "}
                <code className="bg-gray-100 px-1.5 py-0.5 rounded text-sm font-mono">
                  spark.read
                </code>
                . On peut lire depuis une table Delta ou depuis un emplacement
                de fichiers. La lecture crée un DataFrame streaming qui peut être
                manipulé avec les mêmes transformations que les DataFrames
                classiques.
              </p>
              <CodeBlock
                language="python"
                title="Lecture d'un flux streaming"
                code={`# Lire un flux depuis une table Delta
streaming_df = (spark.readStream
    .table("input_table")
)

# Créer une vue temporaire pour requêter en SQL
(spark.readStream
    .table("input_table")
    .createOrReplaceTempView("streaming_tmp_vw")
)

# Requêter la vue streaming comme une table classique
# (mais le résultat reste un DataFrame streaming)
result = spark.sql("SELECT * FROM streaming_tmp_vw WHERE status = 'active'")`}
              />
            </div>

            {/* Writing a stream */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Écrire un flux avec spark.writeStream
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Pour persister les résultats d&apos;un flux, on utilise{" "}
                <code className="bg-gray-100 px-1.5 py-0.5 rounded text-sm font-mono">
                  .writeStream
                </code>
                . Il est essentiel de spécifier un{" "}
                <strong>checkpoint location</strong>, un <strong>mode de sortie</strong>{" "}
                et un <strong>trigger</strong> (déclencheur).
              </p>
              <CodeBlock
                language="python"
                title="Écriture d'un flux streaming"
                code={`# Écrire le résultat d'un flux dans une table Delta
(spark.table("streaming_tmp_vw")
    .writeStream
    .trigger(availableNow=True)
    .outputMode("append")
    .option("checkpointLocation", "/path/to/checkpoint")
    .table("output_table")
    .awaitTermination()
)

# awaitTermination() bloque l'exécution jusqu'à la fin du flux
# (utile avec trigger availableNow ou trigger once)`}
              />
              <InfoBox type="info" title="Tables streaming comme tables classiques">
                <p>
                  Une table Delta alimentée par un flux streaming peut être
                  requêtée comme n&apos;importe quelle table classique. Les
                  utilisateurs en aval n&apos;ont pas besoin de savoir que la
                  table est alimentée en mode streaming.
                </p>
              </InfoBox>
            </div>

            {/* Triggers */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Déclencheurs (Triggers)
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Les déclencheurs contrôlent <strong>quand</strong> le moteur de
                streaming traite les données. Voici les principaux types :
              </p>
              <ul className="list-disc list-inside space-y-3 text-[var(--color-text-light)] mb-4">
                <li>
                  <strong>Non spécifié (par défaut)</strong> : Le prochain
                  micro-batch est déclenché dès que le précédent est terminé.
                  Le flux tourne en continu.
                </li>
                <li>
                  <strong>processingTime</strong> : Le micro-batch est déclenché
                  à intervalles fixes (par exemple toutes les 5 minutes).
                  Exemple :{" "}
                  <code className="bg-gray-100 px-1.5 py-0.5 rounded text-sm font-mono">
                    .trigger(processingTime=&quot;5 minutes&quot;)
                  </code>
                </li>
                <li>
                  <strong>availableNow</strong> : Traite toutes les données
                  disponibles en plusieurs micro-batchs, puis s&apos;arrête.
                  C&apos;est le mode recommandé pour le traitement incrémental
                  planifié. Exemple :{" "}
                  <code className="bg-gray-100 px-1.5 py-0.5 rounded text-sm font-mono">
                    .trigger(availableNow=True)
                  </code>
                </li>
                <li>
                  <strong>once (déprécié)</strong> : Traite toutes les données
                  disponibles en un seul micro-batch, puis s&apos;arrête.
                  Remplacé par <code className="bg-gray-100 px-1.5 py-0.5 rounded text-sm font-mono">availableNow</code>.
                </li>
              </ul>
              <InfoBox type="tip" title="availableNow remplace trigger once">
                <p>
                  Le trigger{" "}
                  <code className="bg-gray-100 px-1.5 py-0.5 rounded text-sm font-mono">
                    availableNow=True
                  </code>{" "}
                  est le successeur de{" "}
                  <code className="bg-gray-100 px-1.5 py-0.5 rounded text-sm font-mono">
                    once=True
                  </code>
                  . La différence principale est que <strong>availableNow</strong>{" "}
                  traite les données en plusieurs micro-batchs (plus efficace
                  pour de grands volumes), tandis que <strong>once</strong> les
                  traitait en un seul batch. Utilisez toujours{" "}
                  <strong>availableNow</strong> dans vos nouveaux projets.
                </p>
              </InfoBox>
              <CodeBlock
                language="python"
                title="Exemples de triggers"
                code={`# Trigger par défaut : continu
(df.writeStream
    .outputMode("append")
    .option("checkpointLocation", "/checkpoint/continuous")
    .table("output_continuous")
)

# Trigger à intervalle fixe
(df.writeStream
    .trigger(processingTime="5 minutes")
    .outputMode("append")
    .option("checkpointLocation", "/checkpoint/interval")
    .table("output_interval")
)

# Trigger availableNow (recommandé pour le traitement planifié)
(df.writeStream
    .trigger(availableNow=True)
    .outputMode("append")
    .option("checkpointLocation", "/checkpoint/available")
    .table("output_available")
    .awaitTermination()
)`}
              />
            </div>

            {/* Checkpointing */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Checkpoints et tolérance aux pannes
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Les <strong>checkpoints</strong> sont le mécanisme qui garantit
                la tolérance aux pannes dans Structured Streaming. Ils
                enregistrent la progression du flux afin de pouvoir reprendre
                exactement là où le traitement s&apos;est arrêté en cas de
                défaillance.
              </p>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Un checkpoint stocke :
              </p>
              <ul className="list-disc list-inside space-y-2 text-[var(--color-text-light)] mb-4">
                <li>
                  <strong>Write-Ahead Logs (WAL)</strong> : Journaux qui
                  enregistrent les offsets des données lues depuis la source.
                  Ils permettent de savoir exactement quelles données ont déjà
                  été traitées.
                </li>
                <li>
                  <strong>State Store</strong> : Stockage de l&apos;état pour
                  les requêtes avec état (agrégations, jointures streaming).
                  Permet de reconstruire l&apos;état intermédiaire après un
                  redémarrage.
                </li>
                <li>
                  <strong>Métadonnées de la requête</strong> : Configuration et
                  plan de la requête streaming.
                </li>
              </ul>
              <InfoBox type="important" title="Checkpoints obligatoires en production">
                <p>
                  Les checkpoints sont <strong>obligatoires</strong> pour tout
                  flux streaming en production. Sans checkpoint, il est
                  impossible de garantir le traitement exact-once (exactly-once)
                  des données. Chaque flux streaming doit avoir son propre
                  répertoire de checkpoint, et ce répertoire ne doit{" "}
                  <strong>jamais être partagé</strong> entre plusieurs flux.
                </p>
              </InfoBox>
            </div>

            {/* Unsupported operations */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Opérations non supportées en streaming
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Certaines opérations ne sont pas supportées sur les DataFrames
                streaming car elles nécessitent un accès à l&apos;ensemble
                complet des données :
              </p>
              <ul className="list-disc list-inside space-y-2 text-[var(--color-text-light)] mb-4">
                <li>
                  <strong>Tri (ORDER BY / sort)</strong> : Impossible de trier
                  un flux infini sans limites.
                </li>
                <li>
                  <strong>Déduplication sur l&apos;ensemble complet</strong> :
                  Le{" "}
                  <code className="bg-gray-100 px-1.5 py-0.5 rounded text-sm font-mono">
                    DISTINCT
                  </code>{" "}
                  global n&apos;est pas supporté. Utilisez{" "}
                  <code className="bg-gray-100 px-1.5 py-0.5 rounded text-sm font-mono">
                    dropDuplicates()
                  </code>{" "}
                  avec un watermark pour la déduplication dans une fenêtre
                  temporelle.
                </li>
                <li>
                  <strong>Agrégations multiples</strong> : Les chaînes
                  d&apos;agrégations ne sont pas supportées.
                </li>
                <li>
                  <strong>LIMIT / TAKE</strong> : Pas de limitation du nombre de
                  lignes sur un flux.
                </li>
              </ul>
            </div>

            {/* Windows and watermarks */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Fenêtres et Watermarks
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Pour gérer les données arrivant en retard (late data) et
                effectuer des agrégations temporelles, Structured Streaming
                propose deux mécanismes :
              </p>
              <ul className="list-disc list-inside space-y-3 text-[var(--color-text-light)] mb-4">
                <li>
                  <strong>Fenêtres temporelles (Windows)</strong> : Permettent
                  de regrouper les données par intervalles de temps. Par
                  exemple, compter les événements par fenêtre de 10 minutes.
                </li>
                <li>
                  <strong>Watermarks</strong> : Définissent un seuil de retard
                  acceptable. Les données arrivant après ce seuil sont ignorées.
                  Cela permet au système de nettoyer l&apos;état ancien et
                  d&apos;éviter une croissance mémoire illimitée.
                </li>
              </ul>
              <CodeBlock
                language="python"
                title="Exemple de fenêtre avec watermark"
                code={`from pyspark.sql.functions import window, col, count

# Agrégation par fenêtre de 10 minutes
# avec un watermark de 30 minutes pour les données en retard
(spark.readStream
    .table("events")
    .withWatermark("event_time", "30 minutes")
    .groupBy(
        window("event_time", "10 minutes"),
        "event_type"
    )
    .agg(count("*").alias("event_count"))
    .writeStream
    .outputMode("update")
    .option("checkpointLocation", "/checkpoint/windowed")
    .table("event_counts")
)`}
              />
            </div>

            {/* Récapitulatif */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Récapitulatif
              </h2>
              <div className="overflow-x-auto my-6">
                <table className="w-full border-collapse border border-gray-200 text-sm">
                  <thead>
                    <tr className="bg-gray-50">
                      <th className="border border-gray-200 px-4 py-3 text-left font-semibold text-[var(--color-text)]">
                        Concept
                      </th>
                      <th className="border border-gray-200 px-4 py-3 text-left font-semibold text-[var(--color-text)]">
                        Description
                      </th>
                    </tr>
                  </thead>
                  <tbody className="text-[var(--color-text-light)]">
                    <tr>
                      <td className="border border-gray-200 px-4 py-3 font-medium">
                        readStream
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Lit des données en mode streaming (table non bornée)
                      </td>
                    </tr>
                    <tr className="bg-gray-50">
                      <td className="border border-gray-200 px-4 py-3 font-medium">
                        writeStream
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Écrit les résultats du flux vers une destination
                      </td>
                    </tr>
                    <tr>
                      <td className="border border-gray-200 px-4 py-3 font-medium">
                        Checkpoint
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Mécanisme de tolérance aux pannes (WAL + State Store)
                      </td>
                    </tr>
                    <tr className="bg-gray-50">
                      <td className="border border-gray-200 px-4 py-3 font-medium">
                        Trigger
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Contrôle le déclenchement du traitement (availableNow, processingTime)
                      </td>
                    </tr>
                    <tr>
                      <td className="border border-gray-200 px-4 py-3 font-medium">
                        Output Mode
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Append (par défaut), Complete, Update
                      </td>
                    </tr>
                    <tr className="bg-gray-50">
                      <td className="border border-gray-200 px-4 py-3 font-medium">
                        Watermark
                      </td>
                      <td className="border border-gray-200 px-4 py-3">
                        Seuil de retard acceptable pour les données tardives
                      </td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </div>
          </section>

          {/* Quiz */}
          <Quiz
            lessonSlug="3-1-structured-streaming"
            title="Quiz — Structured Streaming"
            questions={quizQuestions}
          />

          {/* Exercices */}
          <LessonExercises
            lessonSlug="3-1-structured-streaming"
            exercises={exercises}
          />

          {/* Bouton de complétion */}
          <LessonCompleteButton lessonSlug="3-1-structured-streaming" />

          {/* Navigation */}
          <div className="flex flex-col sm:flex-row justify-between gap-4 mt-12 pt-8 border-t border-[var(--color-border)]">
            <Link
              href="/modules/2-4-fonctions-sql-avancees"
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
              Leçon précédente : Fonctions SQL Avancées
            </Link>
            <Link
              href="/modules/3-2-auto-loader"
              className="inline-flex items-center gap-2 px-5 py-2.5 bg-[#ff3621] text-white rounded-lg font-medium hover:bg-[#e02e1a] transition-colors"
            >
              Leçon suivante : Auto Loader
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
