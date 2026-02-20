"use client";

import Link from "next/link";
import Sidebar from "@/components/Sidebar";
import CodeBlock from "@/components/CodeBlock";
import InfoBox from "@/components/InfoBox";
import Quiz from "@/components/Quiz";
import LessonExercises from "@/components/LessonExercises";
import LessonCompleteButton from "@/components/LessonCompleteButton";

export default function UnityCatalogPage() {
  return (
    <div className="flex min-h-[calc(100vh-4rem)]">
      <Sidebar currentPath="/modules/5-1-unity-catalog" />

      <main className="flex-1 overflow-y-auto">
        <div className="max-w-4xl mx-auto px-6 py-10 lg:px-10">
          {/* Header */}
          <div className="mb-10">
            <div className="flex items-center gap-3 mb-3">
              <span className="inline-flex items-center px-3 py-1 rounded-full text-xs font-semibold bg-blue-100 text-blue-800">
                Module 5
              </span>
              <span className="text-sm text-[var(--color-text-light)]">
                Leçon 5.1
              </span>
            </div>
            <h1 className="text-3xl font-bold text-[var(--color-text)] mb-3">
              Unity Catalog
            </h1>
            <p className="text-lg text-[var(--color-text-light)] leading-relaxed">
              Découvrez Unity Catalog, la solution de gouvernance unifiée de
              Databricks. Apprenez à organiser vos données avec l&apos;espace
              de noms à trois niveaux, à gérer le lignage des données et à
              centraliser la sécurité de votre plateforme de données.
            </p>
          </div>

          {/* Content */}
          <section className="space-y-8">
            {/* Introduction */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Qu&apos;est-ce que Unity Catalog ?
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                <strong>Unity Catalog</strong> est la solution de gouvernance
                des données unifiée de Databricks. Introduit pour répondre aux
                défis croissants de sécurité, de conformité et de gestion des
                données à grande échelle, Unity Catalog offre un point de
                contrôle centralisé pour tous vos actifs de données au sein de
                la plateforme Databricks.
              </p>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Avant Unity Catalog, chaque workspace Databricks possédait son
                propre metastore Hive local, ce qui entraînait plusieurs
                problèmes majeurs :
              </p>
              <ul className="list-disc pl-6 text-[var(--color-text-light)] leading-relaxed space-y-2 mb-4">
                <li>
                  <strong>Isolation des données :</strong> les données
                  n&apos;étaient pas partagées entre les workspaces, ce qui
                  créait des silos.
                </li>
                <li>
                  <strong>Gestion des permissions fragmentée :</strong> les
                  contrôles d&apos;accès étaient définis au niveau de chaque
                  workspace, rendant la gouvernance incohérente.
                </li>
                <li>
                  <strong>Pas de lignage des données :</strong> il était
                  impossible de tracer l&apos;origine et les transformations
                  des données automatiquement.
                </li>
                <li>
                  <strong>Pas d&apos;audit centralisé :</strong> les logs
                  d&apos;accès étaient dispersés et difficiles à consolider.
                </li>
              </ul>

              <InfoBox type="info" title="Solution recommandée">
                Unity Catalog est la solution de gouvernance recommandée par
                Databricks. Elle remplace l&apos;ancien metastore Hive et
                offre une gouvernance fine, centralisée et compatible avec
                l&apos;ensemble de l&apos;écosystème Lakehouse.
              </InfoBox>
            </div>

            {/* Three-level namespace */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                L&apos;espace de noms à trois niveaux
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Unity Catalog introduit un <strong>espace de noms à trois
                niveaux</strong> pour référencer les objets de données. Ce
                système de nommage suit le format :
              </p>
              <div className="bg-gray-100 rounded-lg p-4 mb-4 text-center">
                <code className="text-lg font-mono font-bold text-[var(--color-text)]">
                  catalog.schema.table
                </code>
              </div>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Ce format permet d&apos;identifier de manière unique chaque
                objet de données dans votre environnement, même si des noms
                identiques existent dans différents catalogues ou schémas.
              </p>

              <InfoBox type="important" title="Concept fondamental">
                L&apos;espace de noms à trois niveaux
                (<code>catalog.schema.table</code>) est fondamental dans Unity
                Catalog. Chaque requête doit référencer les objets
                en utilisant ce format complet, ou bien définir un catalogue
                et un schéma par défaut avec les commandes{" "}
                <code>USE CATALOG</code> et <code>USE SCHEMA</code>.
              </InfoBox>
            </div>

            {/* Hierarchy */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Hiérarchie des objets
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Unity Catalog organise les données en une hiérarchie claire,
                du niveau le plus élevé au plus granulaire :
              </p>

              {/* Hierarchy Diagram */}
              <div className="mb-6">
                <div className="border-2 border-purple-300 bg-purple-50 rounded-xl p-4 mb-0">
                  <div className="text-center font-bold text-purple-800 mb-1 text-lg">
                    Metastore
                  </div>
                  <div className="text-center text-sm text-purple-600 mb-3">
                    Conteneur de niveau supérieur — un par région
                  </div>

                  <div className="border-2 border-blue-300 bg-blue-50 rounded-xl p-4 ml-4">
                    <div className="text-center font-bold text-blue-800 mb-1">
                      Catalog
                    </div>
                    <div className="text-center text-sm text-blue-600 mb-3">
                      Premier niveau d&apos;organisation
                    </div>

                    <div className="border-2 border-green-300 bg-green-50 rounded-xl p-4 ml-4">
                      <div className="text-center font-bold text-green-800 mb-1">
                        Schema (Database)
                      </div>
                      <div className="text-center text-sm text-green-600 mb-3">
                        Deuxième niveau d&apos;organisation
                      </div>

                      <div className="flex flex-wrap gap-3 justify-center ml-4">
                        <div className="border-2 border-orange-300 bg-orange-50 rounded-lg px-4 py-2">
                          <div className="text-center font-semibold text-orange-800 text-sm">
                            Tables
                          </div>
                        </div>
                        <div className="border-2 border-orange-300 bg-orange-50 rounded-lg px-4 py-2">
                          <div className="text-center font-semibold text-orange-800 text-sm">
                            Vues
                          </div>
                        </div>
                        <div className="border-2 border-orange-300 bg-orange-50 rounded-lg px-4 py-2">
                          <div className="text-center font-semibold text-orange-800 text-sm">
                            Fonctions
                          </div>
                        </div>
                      </div>
                    </div>
                  </div>
                </div>
              </div>

              {/* Detailed breakdown */}
              <div className="space-y-4">
                <div className="bg-purple-50 border-l-4 border-purple-500 p-4 rounded-r-lg">
                  <h3 className="font-semibold text-purple-800 mb-1">
                    Metastore
                  </h3>
                  <p className="text-[var(--color-text-light)] text-sm leading-relaxed">
                    Le conteneur de niveau supérieur dans Unity Catalog. Il y
                    a généralement <strong>un metastore par région cloud</strong>.
                    Le metastore est attaché à un ou plusieurs workspaces
                    Databricks et stocke les métadonnées de tous les objets de
                    données (catalogues, schémas, tables, etc.) ainsi que les
                    informations de contrôle d&apos;accès.
                  </p>
                </div>
                <div className="bg-blue-50 border-l-4 border-blue-500 p-4 rounded-r-lg">
                  <h3 className="font-semibold text-blue-800 mb-1">
                    Catalog (Catalogue)
                  </h3>
                  <p className="text-[var(--color-text-light)] text-sm leading-relaxed">
                    Le premier niveau d&apos;organisation des données,
                    équivalent à un regroupement logique de bases de données.
                    Les catalogues permettent de séparer les données par
                    environnement (dev, staging, prod), par équipe ou par
                    domaine métier.
                  </p>
                </div>
                <div className="bg-green-50 border-l-4 border-green-500 p-4 rounded-r-lg">
                  <h3 className="font-semibold text-green-800 mb-1">
                    Schema (Schéma / Base de données)
                  </h3>
                  <p className="text-[var(--color-text-light)] text-sm leading-relaxed">
                    Le deuxième niveau d&apos;organisation, équivalent à une
                    base de données classique. Un schéma contient les objets
                    de données réels : tables, vues et fonctions. C&apos;est
                    dans le schéma que vous organisez vos données par domaine
                    fonctionnel.
                  </p>
                </div>
                <div className="bg-orange-50 border-l-4 border-orange-500 p-4 rounded-r-lg">
                  <h3 className="font-semibold text-orange-800 mb-1">
                    Tables, Vues, Fonctions
                  </h3>
                  <p className="text-[var(--color-text-light)] text-sm leading-relaxed">
                    Les objets de données réels contenant ou manipulant les
                    données. Les <strong>tables</strong> stockent les données,
                    les <strong>vues</strong> fournissent des requêtes
                    enregistrées, et les <strong>fonctions</strong> encapsulent
                    de la logique réutilisable.
                  </p>
                </div>
              </div>
            </div>

            {/* Using 3-level namespace */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Utiliser l&apos;espace de noms à trois niveaux
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Voici comment utiliser le système de nommage à trois niveaux
                pour accéder et organiser vos données dans Unity Catalog :
              </p>

              <CodeBlock
                language="sql"
                title="Navigation dans l'espace de noms"
                code={`-- Définir le catalogue par défaut
USE CATALOG my_catalog;

-- Définir le schéma par défaut
USE SCHEMA my_schema;

-- Référence complète à trois niveaux
SELECT * FROM my_catalog.my_schema.my_table;

-- Avec le catalogue et schéma par défaut définis,
-- on peut utiliser directement le nom de la table
SELECT * FROM my_table;`}
              />

              <CodeBlock
                language="sql"
                title="Créer des catalogues et des schémas"
                code={`-- Créer un catalogue
CREATE CATALOG IF NOT EXISTS my_catalog;

-- Créer un schéma dans un catalogue
CREATE SCHEMA IF NOT EXISTS my_catalog.my_schema;

-- Créer un schéma avec un commentaire
CREATE SCHEMA IF NOT EXISTS my_catalog.my_schema
COMMENT 'Schéma pour les données de ventes';

-- Créer une table dans un schéma
CREATE TABLE my_catalog.my_schema.my_table (
  id INT,
  nom STRING,
  date_creation TIMESTAMP
);`}
              />
            </div>

            {/* Managed vs External locations */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Emplacements managés et externes
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Unity Catalog distingue deux types d&apos;emplacements de
                stockage pour les données :
              </p>

              <div className="overflow-x-auto mb-4">
                <table className="w-full border-collapse border border-gray-300 text-sm">
                  <thead>
                    <tr className="bg-gray-100">
                      <th className="border border-gray-300 px-4 py-2 text-left font-semibold text-[var(--color-text)]">
                        Caractéristique
                      </th>
                      <th className="border border-gray-300 px-4 py-2 text-left font-semibold text-[var(--color-text)]">
                        Emplacement managé
                      </th>
                      <th className="border border-gray-300 px-4 py-2 text-left font-semibold text-[var(--color-text)]">
                        Emplacement externe
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Stockage
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Géré par Unity Catalog
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Géré par l&apos;utilisateur
                      </td>
                    </tr>
                    <tr>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Cycle de vie
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Données supprimées avec la table
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Données persistent après suppression
                      </td>
                    </tr>
                    <tr>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Configuration
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Aucune configuration requise
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Nécessite un Storage Credential
                      </td>
                    </tr>
                    <tr>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Cas d&apos;utilisation
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Données internes, développement
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Données partagées, migration
                      </td>
                    </tr>
                  </tbody>
                </table>
              </div>

              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                <strong>Storage Credentials (Identifiants de stockage) :</strong>{" "}
                Un Storage Credential encapsule les identifiants d&apos;accès
                à un emplacement cloud (AWS S3, Azure ADLS, GCS). Il est
                utilisé pour créer des External Locations.
              </p>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                <strong>External Locations (Emplacements externes) :</strong>{" "}
                Un External Location associe un Storage Credential à un chemin
                spécifique dans le stockage cloud, permettant à Unity Catalog
                de contrôler l&apos;accès aux données externes.
              </p>

              <CodeBlock
                language="sql"
                title="Créer une table externe"
                code={`-- Créer une table externe pointant vers un emplacement cloud
CREATE TABLE my_catalog.my_schema.external_table (
  id INT,
  nom STRING
)
LOCATION 's3://my-bucket/data/external_table';`}
              />
            </div>

            {/* Data Lineage */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Lignage des données (Data Lineage)
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                L&apos;une des fonctionnalités les plus puissantes de Unity
                Catalog est le <strong>lignage automatique des
                données</strong>. Unity Catalog capture automatiquement les
                relations entre les tables, vues et notebooks, créant un
                graphe de lignage complet de vos données.
              </p>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Le lignage des données vous permet de :
              </p>
              <ul className="list-disc pl-6 text-[var(--color-text-light)] leading-relaxed space-y-2 mb-4">
                <li>
                  <strong>Tracer l&apos;origine :</strong> identifier d&apos;où
                  proviennent les données d&apos;une table ou d&apos;une
                  colonne.
                </li>
                <li>
                  <strong>Analyser l&apos;impact :</strong> comprendre quels
                  objets seraient affectés par une modification de structure.
                </li>
                <li>
                  <strong>Assurer la conformité :</strong> démontrer le
                  parcours des données sensibles pour les audits
                  réglementaires.
                </li>
                <li>
                  <strong>Déboguer les erreurs :</strong> remonter la chaîne
                  de transformations pour identifier la source d&apos;un
                  problème.
                </li>
              </ul>

              <InfoBox type="tip" title="Lignage automatique">
                Unity Catalog fournit un lignage automatique des données au
                niveau des tables et des colonnes. Chaque fois qu&apos;une
                requête SQL ou un notebook Spark lit ou écrit des données, le
                lignage est capturé sans aucune configuration supplémentaire.
                Vous pouvez visualiser ce lignage directement dans
                l&apos;interface du Data Explorer.
              </InfoBox>
            </div>

            {/* Audit Logging */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Journalisation d&apos;audit
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Unity Catalog enregistre automatiquement un journal d&apos;audit
                détaillé de toutes les actions effectuées sur les objets de
                données. Cela inclut :
              </p>
              <ul className="list-disc pl-6 text-[var(--color-text-light)] leading-relaxed space-y-2 mb-4">
                <li>
                  Les accès en lecture et écriture aux tables et vues.
                </li>
                <li>
                  Les modifications de permissions (GRANT, REVOKE).
                </li>
                <li>
                  Les créations, modifications et suppressions d&apos;objets.
                </li>
                <li>
                  Les connexions et authentifications des utilisateurs.
                </li>
              </ul>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Ces logs d&apos;audit sont essentiels pour la conformité
                réglementaire (RGPD, HIPAA, SOC 2) et pour la détection
                d&apos;activités suspectes.
              </p>
            </div>

            {/* Data Discovery */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Découverte et recherche de données
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Unity Catalog facilite la <strong>découverte des
                données</strong> en fournissant un catalogue centralisé et
                consultable de tous les actifs de données. Les utilisateurs
                peuvent :
              </p>
              <ul className="list-disc pl-6 text-[var(--color-text-light)] leading-relaxed space-y-2 mb-4">
                <li>
                  Rechercher des tables, vues et fonctions par nom ou
                  description.
                </li>
                <li>
                  Parcourir la hiérarchie des catalogues et schémas.
                </li>
                <li>
                  Consulter les métadonnées : schéma de la table, commentaires,
                  tags, propriétaire.
                </li>
                <li>
                  Visualiser des aperçus de données (selon les permissions).
                </li>
                <li>
                  Explorer le lignage des données pour comprendre les flux.
                </li>
              </ul>
            </div>

            {/* Identity Federation */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Fédération d&apos;identités
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Unity Catalog gère trois types de <strong>principaux
                d&apos;identité</strong> (principals) pour contrôler
                l&apos;accès aux données :
              </p>
              <div className="space-y-3 mb-4">
                <div className="bg-gray-50 border border-gray-200 rounded-lg p-4">
                  <h3 className="font-semibold text-[var(--color-text)] mb-1">
                    👤 Utilisateurs (Users)
                  </h3>
                  <p className="text-[var(--color-text-light)] text-sm leading-relaxed">
                    Les comptes individuels identifiés par une adresse e-mail.
                    Ils sont synchronisés depuis votre fournisseur
                    d&apos;identité (Azure AD, Okta, etc.) via SCIM.
                  </p>
                </div>
                <div className="bg-gray-50 border border-gray-200 rounded-lg p-4">
                  <h3 className="font-semibold text-[var(--color-text)] mb-1">
                    👥 Groupes (Groups)
                  </h3>
                  <p className="text-[var(--color-text-light)] text-sm leading-relaxed">
                    Des ensembles d&apos;utilisateurs et/ou d&apos;autres
                    groupes. Les groupes simplifient la gestion des
                    permissions en vous permettant d&apos;accorder des droits
                    à un ensemble d&apos;utilisateurs en une seule opération.
                  </p>
                </div>
                <div className="bg-gray-50 border border-gray-200 rounded-lg p-4">
                  <h3 className="font-semibold text-[var(--color-text)] mb-1">
                    🤖 Service Principals
                  </h3>
                  <p className="text-[var(--color-text-light)] text-sm leading-relaxed">
                    Des identités non-humaines utilisées par les applications,
                    pipelines et services automatisés. Ils permettent
                    d&apos;accorder des accès programmatiques sans utiliser de
                    comptes utilisateur.
                  </p>
                </div>
              </div>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                La fédération d&apos;identités permet de gérer les
                utilisateurs et groupes au niveau du <strong>compte
                Databricks</strong> (account-level), plutôt qu&apos;au niveau
                de chaque workspace individuellement. Cela garantit une
                gestion cohérente des identités à travers tous vos workspaces.
              </p>
            </div>

            {/* Summary */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Résumé
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Unity Catalog est le pilier central de la gouvernance des
                données dans Databricks. Voici les points clés à retenir :
              </p>
              <ul className="list-disc pl-6 text-[var(--color-text-light)] leading-relaxed space-y-2 mb-4">
                <li>
                  L&apos;espace de noms à trois niveaux{" "}
                  <code>catalog.schema.table</code> organise tous les objets
                  de données.
                </li>
                <li>
                  Le metastore est le conteneur de plus haut niveau, attaché
                  aux workspaces d&apos;une région.
                </li>
                <li>
                  Le lignage automatique trace les flux de données au niveau
                  des tables et colonnes.
                </li>
                <li>
                  Les emplacements managés et externes offrent de la
                  flexibilité dans le stockage.
                </li>
                <li>
                  La fédération d&apos;identités centralise la gestion des
                  utilisateurs, groupes et service principals.
                </li>
                <li>
                  La journalisation d&apos;audit garantit la conformité
                  réglementaire.
                </li>
              </ul>
            </div>
          </section>

          {/* Quiz */}
          <Quiz
            lessonSlug="5-1-unity-catalog"
            questions={[
              {
                question: "Quel est l'espace de noms à 3 niveaux dans Unity Catalog ?",
                options: ["database.schema.table", "catalog.schema.table", "metastore.database.table", "workspace.catalog.table"],
                correctIndex: 1,
                explanation: "Unity Catalog utilise un espace de noms à 3 niveaux : catalog.schema.table (ou catalog.schema.view/function)."
              },
              {
                question: "Quel est le conteneur de plus haut niveau dans Unity Catalog ?",
                options: ["Catalog", "Schema", "Metastore", "Workspace"],
                correctIndex: 2,
                explanation: "Le Metastore est au sommet de la hiérarchie. Un Metastore par région, rattaché au workspace. Il contient les Catalogs."
              },
              {
                question: "Que permet le lignage automatique dans Unity Catalog ?",
                options: ["Accélérer les requêtes", "Suivre l'origine et les transformations des données automatiquement", "Créer des backups", "Gérer les permissions"],
                correctIndex: 1,
                explanation: "Le lignage automatique trace d'où viennent les données et comment elles ont été transformées, essentiel pour la gouvernance."
              },
              {
                question: "Qu'est-ce qu'un External Location dans Unity Catalog ?",
                options: ["Une table externe", "Un chemin cloud storage associé à un Storage Credential pour accéder aux données externes", "Un lien vers un autre workspace", "Un alias de base de données"],
                correctIndex: 1,
                explanation: "External Location = chemin cloud storage + Storage Credential. Permet d'accéder aux données stockées en dehors du metastore managé."
              },
              {
                question: "Quelle commande définit le catalog par défaut ?",
                options: ["SET CATALOG my_catalog", "USE CATALOG my_catalog", "DEFAULT CATALOG my_catalog", "SELECT CATALOG my_catalog"],
                correctIndex: 1,
                explanation: "USE CATALOG définit le catalog par défaut pour la session. Ensuite USE SCHEMA pour le schema par défaut."
              }
            ]}
          />

          {/* Exercices */}
          <LessonExercises
            lessonSlug="5-1-unity-catalog"
            exercises={[
              {
                id: "uc-hierarchy",
                title: "Créer une hiérarchie Unity Catalog",
                description: "Créez un catalog, un schema et une table en utilisant l'espace de noms à 3 niveaux.",
                difficulty: "facile" as const,
                type: "code" as const,
                prompt: "Écrivez les commandes SQL pour créer un catalog 'production', un schema 'ventes' dans ce catalog, et une table 'commandes' avec les colonnes id (INT), produit (STRING), montant (DOUBLE) et date_commande (DATE). Utilisez ensuite USE CATALOG et USE SCHEMA pour naviguer, puis faites un SELECT avec le chemin complet.",
                hints: ["Commencez par CREATE CATALOG", "Puis CREATE SCHEMA dans ce catalog", "Enfin CREATE TABLE avec le chemin complet catalog.schema.table"],
                solution: {
                  code: `-- Créer le catalog\nCREATE CATALOG IF NOT EXISTS production;\n\n-- Créer le schema dans le catalog\nCREATE SCHEMA IF NOT EXISTS production.ventes;\n\n-- Créer la table avec le chemin complet\nCREATE TABLE production.ventes.commandes (\n  id INT,\n  produit STRING,\n  montant DOUBLE,\n  date_commande DATE\n);\n\n-- Naviguer avec USE\nUSE CATALOG production;\nUSE SCHEMA ventes;\n\n-- SELECT avec le chemin complet\nSELECT * FROM production.ventes.commandes;`,
                  language: "sql",
                  explanation: "La hiérarchie catalog.schema.table organise les données de manière claire et facilite la gouvernance."
                }
              },
              {
                id: "uc-explore-metastore",
                title: "Explorer le metastore",
                description: "Utilisez les commandes SHOW pour explorer la structure de Unity Catalog.",
                difficulty: "moyen" as const,
                type: "code" as const,
                prompt: "Écrivez les commandes SQL pour lister tous les catalogs disponibles, les schemas dans un catalog 'production', les tables dans le schema 'ventes', et obtenir les détails d'une table 'commandes'.",
                hints: ["SHOW CATALOGS pour lister les catalogs", "SHOW SCHEMAS IN catalog pour les schemas", "SHOW TABLES IN catalog.schema pour les tables"],
                solution: {
                  code: `-- Lister tous les catalogs\nSHOW CATALOGS;\n\n-- Lister les schemas dans un catalog\nSHOW SCHEMAS IN production;\n\n-- Lister les tables dans un schema\nSHOW TABLES IN production.ventes;\n\n-- Obtenir les détails d'une table\nDESCRIBE EXTENDED production.ventes.commandes;`,
                  language: "sql",
                  explanation: "Les commandes SHOW permettent d'explorer la hiérarchie Unity Catalog de manière programmatique."
                }
              }
            ]}
          />

          {/* Bouton de complétion */}
          <LessonCompleteButton
            lessonSlug="5-1-unity-catalog"
          />

          {/* Navigation */}
          <div className="flex justify-between items-center mt-12 pt-8 border-t border-gray-200">
            <Link
              href="/modules/4-3-orchestration-jobs"
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
              Leçon précédente : Orchestration avec Jobs
            </Link>
            <Link
              href="/modules/5-2-gestion-permissions"
              className="inline-flex items-center gap-2 px-5 py-2.5 bg-[#ff3621] text-white rounded-lg font-medium hover:bg-[#e02e1a] transition-colors"
            >
              Leçon suivante : Gestion des Permissions
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
