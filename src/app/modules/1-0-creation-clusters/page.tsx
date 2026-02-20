"use client";

import Link from "next/link";
import Sidebar from "@/components/Sidebar";
import CodeBlock from "@/components/CodeBlock";
import InfoBox from "@/components/InfoBox";
import Quiz from "@/components/Quiz";
import LessonExercises from "@/components/LessonExercises";
import LessonCompleteButton from "@/components/LessonCompleteButton";

export default function CreationClustersPage() {
  return (
    <div className="flex min-h-[calc(100vh-4rem)]">
      <Sidebar currentPath="/modules/1-0-creation-clusters" />

      <main className="flex-1 overflow-y-auto">
        <div className="max-w-4xl mx-auto px-6 py-10 lg:px-10">
          {/* Header */}
          <div className="mb-10">
            <div className="flex items-center gap-3 mb-3">
              <span className="inline-flex items-center px-3 py-1 rounded-full text-xs font-semibold bg-blue-100 text-blue-800">
                Module 1
              </span>
              <span className="text-sm text-[var(--color-text-light)]">
                Leçon 1.0
              </span>
            </div>
            <h1 className="text-3xl font-bold text-[var(--color-text)] mb-3">
              Création de Clusters
            </h1>
            <p className="text-lg text-[var(--color-text-light)] leading-relaxed">
              Apprenez à créer et configurer un cluster Databricks pour exécuter
              vos notebooks et jobs. Comprenez les différents types de clusters
              et leurs cas d&apos;utilisation.
            </p>
          </div>

          {/* Content */}
          <section className="space-y-8">
            {/* Qu'est-ce qu'un cluster ? */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Qu&apos;est-ce qu&apos;un Cluster Databricks ?
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Un <strong>cluster Databricks</strong> est un ensemble de
                ressources de calcul (machines virtuelles) sur lesquelles vous
                exécutez vos notebooks, jobs et pipelines de données. Sans
                cluster, aucun code ne peut être exécuté dans Databricks.
              </p>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Le cluster gère automatiquement la distribution des tâches
                Apache Spark sur plusieurs nœuds, vous permettant de traiter
                de grandes quantités de données de manière efficace.
              </p>
            </div>

            {/* Types de clusters */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Types de Clusters
              </h2>

              <h3 className="text-xl font-medium text-[var(--color-text)] mb-3">
                All-Purpose vs Job Clusters
              </h3>
              <div className="overflow-x-auto mb-6">
                <table className="w-full text-sm border-collapse">
                  <thead>
                    <tr className="bg-gray-50">
                      <th className="text-left px-4 py-3 border border-gray-200 font-semibold text-[var(--color-text)]">
                        Caractéristique
                      </th>
                      <th className="text-left px-4 py-3 border border-gray-200 font-semibold text-[var(--color-text)]">
                        All-Purpose Cluster
                      </th>
                      <th className="text-left px-4 py-3 border border-gray-200 font-semibold text-[var(--color-text)]">
                        Job Cluster
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr>
                      <td className="px-4 py-3 border border-gray-200 text-[var(--color-text-light)]">
                        Utilisation
                      </td>
                      <td className="px-4 py-3 border border-gray-200 text-[var(--color-text-light)]">
                        Développement interactif, exploration de données
                      </td>
                      <td className="px-4 py-3 border border-gray-200 text-[var(--color-text-light)]">
                        Exécution automatisée de jobs en production
                      </td>
                    </tr>
                    <tr className="bg-gray-50">
                      <td className="px-4 py-3 border border-gray-200 text-[var(--color-text-light)]">
                        Cycle de vie
                      </td>
                      <td className="px-4 py-3 border border-gray-200 text-[var(--color-text-light)]">
                        Créé manuellement, peut être réutilisé
                      </td>
                      <td className="px-4 py-3 border border-gray-200 text-[var(--color-text-light)]">
                        Créé et détruit automatiquement par le job
                      </td>
                    </tr>
                    <tr>
                      <td className="px-4 py-3 border border-gray-200 text-[var(--color-text-light)]">
                        Coût
                      </td>
                      <td className="px-4 py-3 border border-gray-200 text-[var(--color-text-light)]">
                        Plus coûteux (tourne en continu)
                      </td>
                      <td className="px-4 py-3 border border-gray-200 text-[var(--color-text-light)]">
                        Plus économique (ne tourne que pendant le job)
                      </td>
                    </tr>
                    <tr className="bg-gray-50">
                      <td className="px-4 py-3 border border-gray-200 text-[var(--color-text-light)]">
                        Partage
                      </td>
                      <td className="px-4 py-3 border border-gray-200 text-[var(--color-text-light)]">
                        Peut être partagé entre plusieurs utilisateurs
                      </td>
                      <td className="px-4 py-3 border border-gray-200 text-[var(--color-text-light)]">
                        Dédié à un seul job
                      </td>
                    </tr>
                  </tbody>
                </table>
              </div>

              <h3 className="text-xl font-medium text-[var(--color-text)] mb-3">
                Single Node vs Multi-Node
              </h3>
              <ul className="list-disc list-inside space-y-2 text-[var(--color-text-light)] mb-4">
                <li>
                  <strong>Single Node :</strong> Un seul nœud qui fait office de
                  driver et de worker. Idéal pour le développement, les tests et
                  les petits jeux de données. C&apos;est ce que nous utiliserons
                  pour ce cours.
                </li>
                <li>
                  <strong>Multi-Node :</strong> Un nœud driver et un ou
                  plusieurs nœuds workers. Recommandé pour le traitement de
                  données volumineuses en production. La charge de travail est
                  distribuée sur tous les workers.
                </li>
              </ul>
            </div>

            {/* Créer un cluster étape par étape */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Créer un Cluster étape par étape
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Suivez ces étapes pour créer votre premier cluster Databricks
                pour ce cours :
              </p>

              <div className="space-y-4">
                <div className="flex gap-4">
                  <div className="flex-shrink-0 w-8 h-8 rounded-full bg-[#ff3621] text-white flex items-center justify-center text-sm font-bold">
                    1
                  </div>
                  <div>
                    <h4 className="font-semibold text-[var(--color-text)] mb-1">
                      Accéder à l&apos;onglet Compute
                    </h4>
                    <p className="text-sm text-[var(--color-text-light)]">
                      Dans la barre latérale gauche de Databricks, cliquez sur
                      l&apos;icône <strong>Compute</strong> (anciennement
                      &quot;Clusters&quot;).
                    </p>
                  </div>
                </div>

                <div className="flex gap-4">
                  <div className="flex-shrink-0 w-8 h-8 rounded-full bg-[#ff3621] text-white flex items-center justify-center text-sm font-bold">
                    2
                  </div>
                  <div>
                    <h4 className="font-semibold text-[var(--color-text)] mb-1">
                      Cliquer sur &quot;Create compute&quot;
                    </h4>
                    <p className="text-sm text-[var(--color-text-light)]">
                      En haut à droite de la page, cliquez sur le bouton bleu{" "}
                      <strong>Create compute</strong>.
                    </p>
                  </div>
                </div>

                <div className="flex gap-4">
                  <div className="flex-shrink-0 w-8 h-8 rounded-full bg-[#ff3621] text-white flex items-center justify-center text-sm font-bold">
                    3
                  </div>
                  <div>
                    <h4 className="font-semibold text-[var(--color-text)] mb-1">
                      Nommer le cluster
                    </h4>
                    <p className="text-sm text-[var(--color-text-light)]">
                      Dans le champ <strong>Cluster name</strong>, entrez :{" "}
                      <code className="bg-gray-100 px-2 py-0.5 rounded text-sm">
                        Demo Cluster
                      </code>
                    </p>
                  </div>
                </div>

                <div className="flex gap-4">
                  <div className="flex-shrink-0 w-8 h-8 rounded-full bg-[#ff3621] text-white flex items-center justify-center text-sm font-bold">
                    4
                  </div>
                  <div>
                    <h4 className="font-semibold text-[var(--color-text)] mb-1">
                      Sélectionner Single Node
                    </h4>
                    <p className="text-sm text-[var(--color-text-light)]">
                      Dans la section <strong>Policy</strong>, sélectionnez{" "}
                      <strong>Single Node</strong> pour un cluster à nœud unique
                      (suffisant pour le cours).
                    </p>
                  </div>
                </div>

                <div className="flex gap-4">
                  <div className="flex-shrink-0 w-8 h-8 rounded-full bg-[#ff3621] text-white flex items-center justify-center text-sm font-bold">
                    5
                  </div>
                  <div>
                    <h4 className="font-semibold text-[var(--color-text)] mb-1">
                      Choisir le Runtime
                    </h4>
                    <p className="text-sm text-[var(--color-text-light)]">
                      Sélectionnez <strong>13.3 LTS</strong> (Long Term Support)
                      comme Databricks Runtime Version. Les versions LTS sont
                      recommandées pour leur stabilité.
                    </p>
                  </div>
                </div>

                <div className="flex gap-4">
                  <div className="flex-shrink-0 w-8 h-8 rounded-full bg-[#ff3621] text-white flex items-center justify-center text-sm font-bold">
                    6
                  </div>
                  <div>
                    <h4 className="font-semibold text-[var(--color-text)] mb-1">
                      Désactiver Photon Acceleration
                    </h4>
                    <p className="text-sm text-[var(--color-text-light)]">
                      Décochez la case <strong>Use Photon Acceleration</strong>.
                      Photon est un moteur de requêtes optimisé mais
                      n&apos;est pas nécessaire pour ce cours.
                    </p>
                  </div>
                </div>

                <div className="flex gap-4">
                  <div className="flex-shrink-0 w-8 h-8 rounded-full bg-[#ff3621] text-white flex items-center justify-center text-sm font-bold">
                    7
                  </div>
                  <div>
                    <h4 className="font-semibold text-[var(--color-text)] mb-1">
                      Type de nœud
                    </h4>
                    <p className="text-sm text-[var(--color-text-light)]">
                      Sélectionnez un type de nœud avec{" "}
                      <strong>4 cores</strong> minimum. Cela offre suffisamment
                      de puissance pour les exercices du cours.
                    </p>
                  </div>
                </div>

                <div className="flex gap-4">
                  <div className="flex-shrink-0 w-8 h-8 rounded-full bg-[#ff3621] text-white flex items-center justify-center text-sm font-bold">
                    8
                  </div>
                  <div>
                    <h4 className="font-semibold text-[var(--color-text)] mb-1">
                      Auto-termination
                    </h4>
                    <p className="text-sm text-[var(--color-text-light)]">
                      Configurez l&apos;arrêt automatique à{" "}
                      <strong>30 minutes</strong> d&apos;inactivité. Le cluster
                      s&apos;éteindra automatiquement après cette durée sans
                      activité.
                    </p>
                  </div>
                </div>

                <div className="flex gap-4">
                  <div className="flex-shrink-0 w-8 h-8 rounded-full bg-[#ff3621] text-white flex items-center justify-center text-sm font-bold">
                    9
                  </div>
                  <div>
                    <h4 className="font-semibold text-[var(--color-text)] mb-1">
                      Créer le cluster
                    </h4>
                    <p className="text-sm text-[var(--color-text-light)]">
                      Cliquez sur <strong>Create compute</strong> en bas de la
                      page. Le cluster va démarrer et sera prêt en quelques
                      minutes (l&apos;indicateur passera au vert).
                    </p>
                  </div>
                </div>
              </div>
            </div>

            {/* Configuration récapitulative */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Récapitulatif de la Configuration
              </h2>
              <div className="overflow-x-auto">
                <table className="w-full text-sm border-collapse">
                  <thead>
                    <tr className="bg-gray-50">
                      <th className="text-left px-4 py-3 border border-gray-200 font-semibold text-[var(--color-text)]">
                        Paramètre
                      </th>
                      <th className="text-left px-4 py-3 border border-gray-200 font-semibold text-[var(--color-text)]">
                        Valeur
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr>
                      <td className="px-4 py-3 border border-gray-200 text-[var(--color-text-light)]">
                        Nom du cluster
                      </td>
                      <td className="px-4 py-3 border border-gray-200 font-mono text-sm">
                        Demo Cluster
                      </td>
                    </tr>
                    <tr className="bg-gray-50">
                      <td className="px-4 py-3 border border-gray-200 text-[var(--color-text-light)]">
                        Mode
                      </td>
                      <td className="px-4 py-3 border border-gray-200 font-mono text-sm">
                        Single Node
                      </td>
                    </tr>
                    <tr>
                      <td className="px-4 py-3 border border-gray-200 text-[var(--color-text-light)]">
                        Runtime
                      </td>
                      <td className="px-4 py-3 border border-gray-200 font-mono text-sm">
                        13.3 LTS (Spark 3.4.1, Scala 2.12)
                      </td>
                    </tr>
                    <tr className="bg-gray-50">
                      <td className="px-4 py-3 border border-gray-200 text-[var(--color-text-light)]">
                        Photon Acceleration
                      </td>
                      <td className="px-4 py-3 border border-gray-200 font-mono text-sm">
                        Désactivé
                      </td>
                    </tr>
                    <tr>
                      <td className="px-4 py-3 border border-gray-200 text-[var(--color-text-light)]">
                        Type de nœud
                      </td>
                      <td className="px-4 py-3 border border-gray-200 font-mono text-sm">
                        4 cores
                      </td>
                    </tr>
                    <tr className="bg-gray-50">
                      <td className="px-4 py-3 border border-gray-200 text-[var(--color-text-light)]">
                        Auto-termination
                      </td>
                      <td className="px-4 py-3 border border-gray-200 font-mono text-sm">
                        30 minutes
                      </td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </div>

            {/* Code example */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Vérifier le Cluster avec du Code
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Une fois le cluster démarré, vous pouvez vérifier qu&apos;il
                fonctionne correctement en exécutant cette commande dans un
                notebook :
              </p>
              <CodeBlock
                language="python"
                title="Vérifier la version de Spark"
                code={`# Vérifier que le cluster est opérationnel
print(f"Version de Spark : {spark.version}")
print(f"Nombre de cores : {sc.defaultParallelism}")
print(f"Application ID : {sc.applicationId}")`}
              />
              <CodeBlock
                language="sql"
                title="Vérifier via SQL"
                code={`-- Vérifier la version du runtime
SELECT current_version();

-- Lister les bases de données disponibles
SHOW DATABASES;`}
              />
            </div>

            {/* InfoBoxes */}
            <InfoBox type="tip" title="Astuce : Auto-termination pour économiser">
              Configurez toujours l&apos;auto-termination sur vos clusters de
              développement. Cela permet d&apos;éviter de laisser un cluster
              tourner inutilement et de réduire les coûts. Pour un usage en
              cours, <strong>30 minutes</strong> est un bon compromis entre
              confort et économie.
            </InfoBox>

            <InfoBox type="important" title="Choisir la bonne version du Runtime">
              Utilisez toujours une version <strong>LTS</strong> (Long Term
              Support) du runtime Databricks. Les versions LTS bénéficient de
              correctifs de sécurité et de stabilité pendant une durée prolongée.
              La version <strong>13.3 LTS</strong> est recommandée pour ce cours
              car elle offre un excellent équilibre entre fonctionnalités et
              stabilité.
            </InfoBox>

            <InfoBox type="info" title="Temps de démarrage du cluster">
              Le démarrage d&apos;un cluster prend généralement entre{" "}
              <strong>3 et 7 minutes</strong>. Pendant ce temps, Databricks
              provisionne les ressources cloud nécessaires. L&apos;indicateur
              passe au vert une fois le cluster prêt à l&apos;emploi.
            </InfoBox>
          </section>

          {/* Quiz & Exercises */}
          <Quiz
            lessonSlug="1-0-creation-clusters"
            title="Quiz : Création de Clusters"
            questions={[
              {
                question: "Quelle est la différence principale entre un cluster All-Purpose et un Job cluster ?",
                options: [
                  "Le All-Purpose est moins cher",
                  "Le Job cluster est créé et détruit automatiquement pour une tâche spécifique",
                  "Le All-Purpose ne supporte pas Python",
                  "Le Job cluster est plus lent"
                ],
                correctIndex: 1,
                explanation: "Un Job cluster est créé automatiquement au début d'un job et détruit à la fin, ce qui optimise les coûts. Le All-Purpose reste actif pour le développement interactif."
              },
              {
                question: "Pourquoi configurer l'auto-termination sur un cluster ?",
                options: [
                  "Pour améliorer les performances",
                  "Pour éviter les coûts inutiles quand le cluster est inactif",
                  "Pour augmenter la mémoire disponible",
                  "C'est obligatoire dans Databricks"
                ],
                correctIndex: 1,
                explanation: "L'auto-termination arrête automatiquement le cluster après une période d'inactivité, évitant ainsi des coûts cloud inutiles."
              },
              {
                question: "Quelle est la différence entre un cluster Single Node et Multi-Node ?",
                options: [
                  "Single Node n'a qu'un driver sans workers, Multi-Node a un driver + workers",
                  "Multi-Node est réservé aux administrateurs",
                  "Single Node est plus performant",
                  "Il n'y a aucune différence"
                ],
                correctIndex: 0,
                explanation: "Un Single Node cluster n'a qu'un driver (idéal pour le développement), tandis qu'un Multi-Node a un driver + workers pour le traitement distribué."
              },
              {
                question: "Que signifie 'LTS' dans la version du runtime Databricks ?",
                options: [
                  "Low Throughput System",
                  "Long Term Support - support à long terme",
                  "Large Table Storage",
                  "Live Tracking Service"
                ],
                correctIndex: 1,
                explanation: "LTS signifie Long Term Support. Ces versions reçoivent des correctifs de sécurité et de stabilité plus longtemps, ce qui les rend recommandées pour la production."
              },
              {
                question: "Qu'est-ce que Photon dans Databricks ?",
                options: [
                  "Un type de cluster spécial",
                  "Un moteur d'exécution natif C++ qui accélère les requêtes SQL et Spark",
                  "Un outil de monitoring",
                  "Un langage de programmation"
                ],
                correctIndex: 1,
                explanation: "Photon est un moteur d'exécution vectorisé écrit en C++ qui accélère significativement les charges de travail SQL et DataFrame, mais il coûte plus cher."
              }
            ]}
          />

          <LessonExercises
            lessonSlug="1-0-creation-clusters"
            exercises={[
              {
                id: "ex1",
                title: "Planifier une configuration cluster",
                description: "Choisir la bonne configuration selon le cas d'usage",
                difficulty: "facile",
                type: "reflexion",
                prompt: "Votre équipe a besoin de :\n1. Un cluster pour le développement interactif de notebooks\n2. Un cluster pour un job ETL quotidien\n\nPour chaque besoin, précisez : type de cluster, Single/Multi-Node, et justifiez.",
                hints: [
                  "Pensez au coût : quel type se détruit automatiquement ?",
                  "Pour le développement, a-t-on besoin de beaucoup de workers ?",
                  "All-Purpose = interactif, Job = automatisé"
                ],
                solution: {
                  explanation: "1. Développement : cluster All-Purpose, Single Node (suffisant pour explorer les données et développer). 2. Job ETL : Job cluster, Multi-Node (créé/détruit automatiquement, traitement distribué pour les gros volumes). Le Job cluster est plus économique car il ne tourne que pendant l'exécution du job."
                }
              },
              {
                id: "ex2",
                title: "Calculer les économies d'auto-termination",
                description: "Comprendre l'impact financier de l'auto-termination",
                difficulty: "moyen",
                type: "reflexion",
                prompt: "Un cluster coûte 2€/heure. Votre développeur travaille de 9h à 18h (9h de travail effectif) mais oublie souvent d'éteindre son cluster le soir.\n\nCalculez :\n1. Le coût mensuel SANS auto-termination (cluster tourne 24/7, 22 jours ouvrés)\n2. Le coût mensuel AVEC auto-termination de 30 minutes\n3. Les économies réalisées",
                hints: [
                  "Sans auto-termination : 24h × 22 jours × 2€",
                  "Avec auto-termination : (9h + 0.5h) × 22 jours × 2€",
                  "L'économie est la différence entre les deux"
                ],
                solution: {
                  explanation: "Sans auto-termination : 24 × 22 × 2€ = 1 056€/mois. Avec auto-termination (30 min) : 9.5 × 22 × 2€ = 418€/mois. Économie : 638€/mois soit 60% de réduction ! C'est pourquoi l'auto-termination est indispensable."
                }
              }
            ]}
          />

          <LessonCompleteButton lessonSlug="1-0-creation-clusters" />

          {/* Navigation */}
          <div className="flex items-center justify-end mt-12 pt-6 border-t border-gray-200">
            <Link
              href="/modules/1-1-bases-notebooks"
              className="inline-flex items-center gap-2 px-5 py-2.5 bg-[#ff3621] text-white rounded-lg font-medium hover:bg-[#e02e1a] transition-colors"
            >
              Leçon suivante : Bases des Notebooks
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
