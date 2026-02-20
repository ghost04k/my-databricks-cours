"use client";

import Link from "next/link";
import Sidebar from "@/components/Sidebar";
import CodeBlock from "@/components/CodeBlock";
import InfoBox from "@/components/InfoBox";
import Quiz from "@/components/Quiz";
import LessonExercises from "@/components/LessonExercises";
import LessonCompleteButton from "@/components/LessonCompleteButton";

export default function GestionPermissionsPage() {
  return (
    <div className="flex min-h-[calc(100vh-4rem)]">
      <Sidebar currentPath="/modules/5-2-gestion-permissions" />

      <main className="flex-1 overflow-y-auto">
        <div className="max-w-4xl mx-auto px-6 py-10 lg:px-10">
          {/* Header */}
          <div className="mb-10">
            <div className="flex items-center gap-3 mb-3">
              <span className="inline-flex items-center px-3 py-1 rounded-full text-xs font-semibold bg-blue-100 text-blue-800">
                Module 5
              </span>
              <span className="text-sm text-[var(--color-text-light)]">
                Leçon 5.2
              </span>
            </div>
            <h1 className="text-3xl font-bold text-[var(--color-text)] mb-3">
              Gestion des Permissions
            </h1>
            <p className="text-lg text-[var(--color-text-light)] leading-relaxed">
              Apprenez à sécuriser vos données avec le modèle de permissions
              de Unity Catalog. Maîtrisez les commandes GRANT et REVOKE, la
              hiérarchie des privilèges, et les techniques de sécurité au
              niveau des lignes et des colonnes avec les vues dynamiques.
            </p>
          </div>

          {/* Content */}
          <section className="space-y-8">
            {/* Permission Model */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Le modèle de permissions de Unity Catalog
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Unity Catalog utilise un modèle de permissions déclaratif basé
                sur SQL. Ce modèle repose sur trois concepts fondamentaux :
                les <strong>principaux</strong> (qui accède), les{" "}
                <strong>objets sécurisables</strong> (à quoi on accède), et
                les <strong>privilèges</strong> (quel type d&apos;accès).
              </p>
            </div>

            {/* Principals */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Les principaux (Principals)
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Un <strong>principal</strong> est une entité à laquelle on
                peut accorder des permissions. Unity Catalog reconnaît trois
                types de principaux :
              </p>
              <div className="overflow-x-auto mb-4">
                <table className="w-full border-collapse border border-gray-300 text-sm">
                  <thead>
                    <tr className="bg-gray-100">
                      <th className="border border-gray-300 px-4 py-2 text-left font-semibold text-[var(--color-text)]">
                        Principal
                      </th>
                      <th className="border border-gray-300 px-4 py-2 text-left font-semibold text-[var(--color-text)]">
                        Description
                      </th>
                      <th className="border border-gray-300 px-4 py-2 text-left font-semibold text-[var(--color-text)]">
                        Exemple
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr>
                      <td className="border border-gray-300 px-4 py-2 font-semibold text-[var(--color-text-light)]">
                        Utilisateur
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Un compte individuel identifié par e-mail
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        <code>user@entreprise.com</code>
                      </td>
                    </tr>
                    <tr>
                      <td className="border border-gray-300 px-4 py-2 font-semibold text-[var(--color-text-light)]">
                        Groupe
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Un ensemble d&apos;utilisateurs et/ou de groupes
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        <code>data_engineers</code>
                      </td>
                    </tr>
                    <tr>
                      <td className="border border-gray-300 px-4 py-2 font-semibold text-[var(--color-text-light)]">
                        Service Principal
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Identité non-humaine pour l&apos;automatisation
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        <code>pipeline-etl-sp</code>
                      </td>
                    </tr>
                  </tbody>
                </table>
              </div>

              <InfoBox type="tip" title="Utilisez les groupes">
                Il est fortement recommandé d&apos;utiliser des{" "}
                <strong>groupes</strong> plutôt que des utilisateurs
                individuels pour gérer les permissions. Cela simplifie
                l&apos;administration, facilite l&apos;onboarding/offboarding
                et réduit les risques d&apos;erreur. Créez des groupes par
                rôle (ex : <code>data_analysts</code>,{" "}
                <code>data_engineers</code>, <code>admins</code>).
              </InfoBox>
            </div>

            {/* Securable Objects */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Les objets sécurisables
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Les <strong>objets sécurisables</strong> sont les ressources
                sur lesquelles on peut définir des permissions. Ils suivent la
                hiérarchie de Unity Catalog :
              </p>
              <ul className="list-disc pl-6 text-[var(--color-text-light)] leading-relaxed space-y-2 mb-4">
                <li>
                  <strong>Metastore :</strong> le conteneur de plus haut
                  niveau.
                </li>
                <li>
                  <strong>Catalog :</strong> un regroupement logique de
                  schémas.
                </li>
                <li>
                  <strong>Schema :</strong> un regroupement logique de tables,
                  vues et fonctions.
                </li>
                <li>
                  <strong>Table :</strong> une table de données (managée ou
                  externe).
                </li>
                <li>
                  <strong>View :</strong> une vue basée sur une requête SQL.
                </li>
                <li>
                  <strong>Function :</strong> une fonction définie par
                  l&apos;utilisateur (UDF).
                </li>
              </ul>
            </div>

            {/* Privileges */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Les privilèges
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Les <strong>privilèges</strong> définissent le type
                d&apos;accès accordé à un principal sur un objet sécurisable.
                Voici les principaux privilèges disponibles :
              </p>
              <div className="overflow-x-auto mb-4">
                <table className="w-full border-collapse border border-gray-300 text-sm">
                  <thead>
                    <tr className="bg-gray-100">
                      <th className="border border-gray-300 px-4 py-2 text-left font-semibold text-[var(--color-text)]">
                        Privilège
                      </th>
                      <th className="border border-gray-300 px-4 py-2 text-left font-semibold text-[var(--color-text)]">
                        Description
                      </th>
                      <th className="border border-gray-300 px-4 py-2 text-left font-semibold text-[var(--color-text)]">
                        S&apos;applique à
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr>
                      <td className="border border-gray-300 px-4 py-2 font-mono font-semibold text-[var(--color-text-light)]">
                        SELECT
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Lire les données d&apos;une table ou vue
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Table, Vue
                      </td>
                    </tr>
                    <tr>
                      <td className="border border-gray-300 px-4 py-2 font-mono font-semibold text-[var(--color-text-light)]">
                        MODIFY
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Insérer, mettre à jour, supprimer des données
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Table
                      </td>
                    </tr>
                    <tr>
                      <td className="border border-gray-300 px-4 py-2 font-mono font-semibold text-[var(--color-text-light)]">
                        CREATE
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Créer des objets enfants
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Catalog, Schema
                      </td>
                    </tr>
                    <tr>
                      <td className="border border-gray-300 px-4 py-2 font-mono font-semibold text-[var(--color-text-light)]">
                        USAGE
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Accéder aux objets contenus dans le conteneur
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Catalog, Schema
                      </td>
                    </tr>
                    <tr>
                      <td className="border border-gray-300 px-4 py-2 font-mono font-semibold text-[var(--color-text-light)]">
                        ALL PRIVILEGES
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Tous les privilèges applicables
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Tous les objets
                      </td>
                    </tr>
                  </tbody>
                </table>
              </div>

              <InfoBox type="warning" title="Attention : ALL PRIVILEGES">
                Le privilège <code>ALL PRIVILEGES</code> accorde non seulement
                tous les privilèges actuels, mais aussi tous les{" "}
                <strong>privilèges futurs</strong> qui pourraient être ajoutés
                par Databricks. Utilisez-le avec précaution et préférez
                toujours accorder les privilèges spécifiques nécessaires.
              </InfoBox>
            </div>

            {/* GRANT and REVOKE */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Commandes GRANT et REVOKE
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Les commandes <code>GRANT</code> et <code>REVOKE</code> sont
                les outils principaux pour gérer les permissions dans Unity
                Catalog. Voici les syntaxes et exemples :
              </p>

              <CodeBlock
                language="sql"
                title="Accorder des permissions (GRANT)"
                code={`-- Accorder le SELECT sur une table à un utilisateur
GRANT SELECT ON TABLE catalog.schema.table TO \`user@email.com\`;

-- Accorder USAGE sur un schéma (requis pour accéder aux objets contenus)
GRANT USAGE ON SCHEMA catalog.schema TO \`group_name\`;

-- Accorder USAGE sur un catalogue
GRANT USAGE ON CATALOG catalog TO \`group_name\`;

-- Accorder CREATE TABLE sur un schéma
GRANT CREATE TABLE ON SCHEMA catalog.schema TO \`user@email.com\`;

-- Accorder plusieurs privilèges en une seule commande
GRANT SELECT, MODIFY ON TABLE catalog.schema.table TO \`data_engineers\`;`}
              />

              <CodeBlock
                language="sql"
                title="Révoquer des permissions (REVOKE)"
                code={`-- Révoquer le SELECT sur une table
REVOKE SELECT ON TABLE catalog.schema.table FROM \`user@email.com\`;

-- Révoquer tous les privilèges
REVOKE ALL PRIVILEGES ON TABLE catalog.schema.table FROM \`user@email.com\`;`}
              />

              <CodeBlock
                language="sql"
                title="Consulter les permissions (SHOW GRANTS)"
                code={`-- Voir les permissions sur une table
SHOW GRANTS ON TABLE catalog.schema.table;

-- Voir les permissions accordées à un utilisateur
SHOW GRANTS TO \`user@email.com\`;

-- Voir les permissions sur un schéma
SHOW GRANTS ON SCHEMA catalog.schema;

-- Voir les permissions sur un catalogue
SHOW GRANTS ON CATALOG catalog;`}
              />
            </div>

            {/* Privilege Inheritance */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Héritage des privilèges et USAGE
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Pour qu&apos;un utilisateur puisse accéder à une table, il
                doit avoir le privilège <code>USAGE</code> à{" "}
                <strong>chaque niveau</strong> de la hiérarchie, en plus du
                privilège spécifique sur l&apos;objet cible. Voici comment
                fonctionne la chaîne d&apos;accès :
              </p>

              {/* Access chain diagram */}
              <div className="flex flex-col sm:flex-row items-center gap-2 justify-center mb-6">
                <div className="bg-blue-100 border-2 border-blue-300 rounded-lg px-4 py-2 text-center">
                  <div className="font-bold text-blue-800 text-sm">
                    USAGE
                  </div>
                  <div className="text-xs text-blue-600">sur Catalog</div>
                </div>
                <span className="text-2xl text-gray-400">→</span>
                <div className="bg-green-100 border-2 border-green-300 rounded-lg px-4 py-2 text-center">
                  <div className="font-bold text-green-800 text-sm">
                    USAGE
                  </div>
                  <div className="text-xs text-green-600">sur Schema</div>
                </div>
                <span className="text-2xl text-gray-400">→</span>
                <div className="bg-orange-100 border-2 border-orange-300 rounded-lg px-4 py-2 text-center">
                  <div className="font-bold text-orange-800 text-sm">
                    SELECT
                  </div>
                  <div className="text-xs text-orange-600">sur Table</div>
                </div>
              </div>

              <InfoBox type="important" title="USAGE n'est PAS hérité">
                Le privilège <code>USAGE</code> n&apos;est{" "}
                <strong>PAS hérité</strong> dans la hiérarchie. Accorder{" "}
                <code>USAGE</code> sur un catalogue ne donne{" "}
                <strong>pas</strong> automatiquement <code>USAGE</code> sur
                ses schémas. Vous devez explicitement accorder{" "}
                <code>USAGE</code> à <strong>chaque niveau</strong> de la
                hiérarchie pour que l&apos;utilisateur puisse accéder aux
                objets enfants.
              </InfoBox>

              <CodeBlock
                language="sql"
                title="Exemple complet d'accès à une table"
                code={`-- Étape 1 : Accorder USAGE sur le catalogue
GRANT USAGE ON CATALOG production TO \`data_analysts\`;

-- Étape 2 : Accorder USAGE sur le schéma
GRANT USAGE ON SCHEMA production.ventes TO \`data_analysts\`;

-- Étape 3 : Accorder SELECT sur la table
GRANT SELECT ON TABLE production.ventes.commandes TO \`data_analysts\`;

-- Sans les étapes 1 et 2, l'étape 3 seule ne suffit PAS !`}
              />

              {/* Privilege hierarchy table */}
              <div className="overflow-x-auto mt-4">
                <table className="w-full border-collapse border border-gray-300 text-sm">
                  <thead>
                    <tr className="bg-gray-100">
                      <th className="border border-gray-300 px-4 py-2 text-left font-semibold text-[var(--color-text)]">
                        Action souhaitée
                      </th>
                      <th className="border border-gray-300 px-4 py-2 text-left font-semibold text-[var(--color-text)]">
                        Permissions requises
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Lire une table
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        <code>USAGE</code> sur Catalog +{" "}
                        <code>USAGE</code> sur Schema +{" "}
                        <code>SELECT</code> sur Table
                      </td>
                    </tr>
                    <tr>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Modifier les données d&apos;une table
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        <code>USAGE</code> sur Catalog +{" "}
                        <code>USAGE</code> sur Schema +{" "}
                        <code>MODIFY</code> sur Table
                      </td>
                    </tr>
                    <tr>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Créer une table dans un schéma
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        <code>USAGE</code> sur Catalog +{" "}
                        <code>USAGE</code> sur Schema +{" "}
                        <code>CREATE TABLE</code> sur Schema
                      </td>
                    </tr>
                    <tr>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Créer un schéma dans un catalogue
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        <code>USAGE</code> sur Catalog +{" "}
                        <code>CREATE SCHEMA</code> sur Catalog
                      </td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </div>

            {/* Dynamic Views */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Vues dynamiques pour la sécurité fine
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Les <strong>vues dynamiques</strong> permettent
                d&apos;implémenter une sécurité au niveau des lignes
                (row-level security) et des colonnes (column-level security).
                Elles utilisent des fonctions d&apos;identité pour filtrer ou
                masquer les données en fonction de l&apos;utilisateur
                connecté.
              </p>

              <h3 className="text-xl font-semibold text-[var(--color-text)] mb-3">
                Sécurité au niveau des lignes (Row-Level Security)
              </h3>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Filtrer les lignes en fonction de l&apos;identité de
                l&apos;utilisateur connecté :
              </p>
              <CodeBlock
                language="sql"
                title="Vue avec sécurité au niveau des lignes"
                code={`-- Chaque utilisateur ne voit que les données de son département
CREATE OR REPLACE VIEW catalog.schema.secure_employees AS
SELECT *
FROM catalog.schema.employees
WHERE department = current_user();

-- Filtrage basé sur l'appartenance à un groupe
CREATE OR REPLACE VIEW catalog.schema.secure_sales AS
SELECT *
FROM catalog.schema.sales
WHERE
  is_member('admins')           -- Les admins voient tout
  OR region = current_user();   -- Les autres ne voient que leur région`}
              />

              <h3 className="text-xl font-semibold text-[var(--color-text)] mb-3 mt-6">
                Sécurité au niveau des colonnes (Column-Level Security)
              </h3>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Masquer ou transformer certaines colonnes sensibles en
                fonction du rôle de l&apos;utilisateur :
              </p>
              <CodeBlock
                language="sql"
                title="Vue avec masquage de colonnes"
                code={`-- Masquer les colonnes sensibles pour les non-admins
CREATE OR REPLACE VIEW catalog.schema.masked_employees AS
SELECT
  id,
  nom,
  email,
  CASE
    WHEN is_member('admins') THEN numero_secu
    ELSE 'XXXXXXXXX'
  END AS numero_secu,
  CASE
    WHEN is_member('rh') OR is_member('admins') THEN salaire
    ELSE NULL
  END AS salaire,
  departement
FROM catalog.schema.employees;`}
              />

              <h3 className="text-xl font-semibold text-[var(--color-text)] mb-3 mt-6">
                Fonctions d&apos;identité
              </h3>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Unity Catalog fournit des fonctions intégrées pour identifier
                l&apos;utilisateur connecté :
              </p>
              <div className="overflow-x-auto mb-4">
                <table className="w-full border-collapse border border-gray-300 text-sm">
                  <thead>
                    <tr className="bg-gray-100">
                      <th className="border border-gray-300 px-4 py-2 text-left font-semibold text-[var(--color-text)]">
                        Fonction
                      </th>
                      <th className="border border-gray-300 px-4 py-2 text-left font-semibold text-[var(--color-text)]">
                        Description
                      </th>
                      <th className="border border-gray-300 px-4 py-2 text-left font-semibold text-[var(--color-text)]">
                        Retour
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr>
                      <td className="border border-gray-300 px-4 py-2 font-mono text-[var(--color-text-light)]">
                        current_user()
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Retourne l&apos;adresse e-mail de l&apos;utilisateur
                        connecté
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        STRING
                      </td>
                    </tr>
                    <tr>
                      <td className="border border-gray-300 px-4 py-2 font-mono text-[var(--color-text-light)]">
                        is_member(&apos;group&apos;)
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        Vérifie si l&apos;utilisateur est membre du groupe
                        spécifié
                      </td>
                      <td className="border border-gray-300 px-4 py-2 text-[var(--color-text-light)]">
                        BOOLEAN
                      </td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </div>

            {/* Ownership */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Concept de propriétaire (Owner)
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Chaque objet dans Unity Catalog a un{" "}
                <strong>propriétaire</strong> (OWNER). Le propriétaire possède
                un contrôle total sur l&apos;objet, incluant :
              </p>
              <ul className="list-disc pl-6 text-[var(--color-text-light)] leading-relaxed space-y-2 mb-4">
                <li>
                  Tous les privilèges sur l&apos;objet (SELECT, MODIFY, etc.).
                </li>
                <li>
                  Le droit de <strong>GRANT</strong> et <strong>REVOKE</strong>{" "}
                  des permissions à d&apos;autres principaux.
                </li>
                <li>
                  Le droit de <strong>supprimer</strong> (DROP) l&apos;objet.
                </li>
                <li>
                  Le droit de <strong>transférer la propriété</strong> à un
                  autre principal.
                </li>
              </ul>

              <CodeBlock
                language="sql"
                title="Transférer la propriété"
                code={`-- Transférer la propriété d'une table
ALTER TABLE catalog.schema.table SET OWNER TO \`new_owner@email.com\`;

-- Transférer la propriété d'un schéma
ALTER SCHEMA catalog.schema SET OWNER TO \`data_engineers\`;`}
              />
            </div>

            {/* Best Practices */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Bonnes pratiques
              </h2>
              <div className="space-y-3 mb-4">
                <div className="bg-gray-50 border border-gray-200 rounded-lg p-4">
                  <h3 className="font-semibold text-[var(--color-text)] mb-1">
                    🔒 Principe du moindre privilège
                  </h3>
                  <p className="text-[var(--color-text-light)] text-sm leading-relaxed">
                    Accordez uniquement les permissions minimales nécessaires
                    pour que l&apos;utilisateur puisse accomplir sa tâche.
                    Évitez <code>ALL PRIVILEGES</code> sauf pour les
                    administrateurs.
                  </p>
                </div>
                <div className="bg-gray-50 border border-gray-200 rounded-lg p-4">
                  <h3 className="font-semibold text-[var(--color-text)] mb-1">
                    👥 Utilisez les groupes
                  </h3>
                  <p className="text-[var(--color-text-light)] text-sm leading-relaxed">
                    Gérez les permissions via des groupes plutôt que des
                    utilisateurs individuels. Cela simplifie grandement
                    l&apos;administration et assure la cohérence.
                  </p>
                </div>
                <div className="bg-gray-50 border border-gray-200 rounded-lg p-4">
                  <h3 className="font-semibold text-[var(--color-text)] mb-1">
                    📋 Documentez vos permissions
                  </h3>
                  <p className="text-[var(--color-text-light)] text-sm leading-relaxed">
                    Utilisez régulièrement <code>SHOW GRANTS</code> pour
                    auditer les permissions accordées et maintenez une
                    documentation des rôles et accès.
                  </p>
                </div>
                <div className="bg-gray-50 border border-gray-200 rounded-lg p-4">
                  <h3 className="font-semibold text-[var(--color-text)] mb-1">
                    🛡️ Vues dynamiques pour les données sensibles
                  </h3>
                  <p className="text-[var(--color-text-light)] text-sm leading-relaxed">
                    Utilisez des vues avec <code>current_user()</code> et{" "}
                    <code>is_member()</code> pour implémenter la sécurité au
                    niveau des lignes et des colonnes sans dupliquer les
                    données.
                  </p>
                </div>
              </div>
            </div>

            {/* Summary */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Résumé
              </h2>
              <ul className="list-disc pl-6 text-[var(--color-text-light)] leading-relaxed space-y-2 mb-4">
                <li>
                  Le modèle de permissions repose sur les{" "}
                  <strong>principaux</strong>, les{" "}
                  <strong>objets sécurisables</strong> et les{" "}
                  <strong>privilèges</strong>.
                </li>
                <li>
                  <code>GRANT</code> accorde des permissions,{" "}
                  <code>REVOKE</code> les retire.
                </li>
                <li>
                  <code>USAGE</code> est requis à chaque niveau de la
                  hiérarchie et n&apos;est pas hérité.
                </li>
                <li>
                  Les vues dynamiques permettent une sécurité fine au niveau
                  des lignes et des colonnes.
                </li>
                <li>
                  Le propriétaire (OWNER) a un contrôle total sur ses objets.
                </li>
                <li>
                  Privilégiez les groupes et le principe du moindre privilège.
                </li>
              </ul>
            </div>

            {/* Congratulations */}
            <div className="bg-gradient-to-r from-green-50 to-emerald-50 border-2 border-green-300 rounded-xl p-8 text-center">
              <div className="text-4xl mb-4">🎉</div>
              <h2 className="text-2xl font-bold text-green-800 mb-3">
                Félicitations !
              </h2>
              <p className="text-green-700 leading-relaxed mb-4 text-lg">
                Vous avez terminé toutes les leçons du cours Databricks.
                Vous maîtrisez maintenant les fondamentaux de la plateforme
                Lakehouse, du traitement de données avec Spark SQL, du
                streaming, des pipelines de production et de la gouvernance
                des données avec Unity Catalog.
              </p>
              <Link
                href="/"
                className="inline-flex items-center gap-2 px-6 py-3 bg-green-600 text-white rounded-lg font-semibold hover:bg-green-700 transition-colors"
              >
                <svg
                  className="w-5 h-5"
                  fill="none"
                  stroke="currentColor"
                  viewBox="0 0 24 24"
                >
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    strokeWidth={2}
                    d="M3 12l2-2m0 0l7-7 7 7M5 10v10a1 1 0 001 1h3m10-11l2 2m-2-2v10a1 1 0 01-1 1h-3m-4 0h4"
                  />
                </svg>
                Retour à l&apos;accueil
              </Link>
            </div>
          </section>

          {/* Quiz */}
          <Quiz
            lessonSlug="5-2-gestion-permissions"
            questions={[
              {
                question: "Le privilège USAGE est-il hérité dans Unity Catalog ?",
                options: ["Oui, il se propage automatiquement", "Non, il faut le donner à CHAQUE niveau (catalog ET schema)", "Seulement pour les admins", "Seulement vers le bas"],
                correctIndex: 1,
                explanation: "USAGE n'est PAS hérité ! Il faut explicitement GRANT USAGE sur le catalog ET sur le schema pour accéder aux tables. C'est un point clé de l'examen."
              },
              {
                question: "Quelle commande voir les permissions sur une table ?",
                options: ["DESCRIBE PERMISSIONS table", "SHOW GRANTS ON TABLE table", "LIST ACCESS table", "GET PERMISSIONS table"],
                correctIndex: 1,
                explanation: "SHOW GRANTS ON TABLE montre toutes les permissions accordées. SHOW GRANTS TO user montre les permissions d'un utilisateur."
              },
              {
                question: "Comment implémenter la sécurité au niveau ligne (row-level security) ?",
                options: ["Avec des filtres sur la table", "Avec une vue dynamique utilisant current_user() ou is_member()", "Avec des permissions spéciales", "Ce n'est pas possible"],
                correctIndex: 1,
                explanation: "Les vues dynamiques avec current_user() et is_member() permettent de filtrer les lignes selon l'identité de l'utilisateur."
              },
              {
                question: "Quelle est la bonne pratique pour gérer les permissions ?",
                options: ["Donner ALL PRIVILEGES à tout le monde", "Utiliser des groupes plutôt que des utilisateurs individuels", "Ne pas utiliser de permissions", "Donner les permissions uniquement au niveau catalog"],
                correctIndex: 1,
                explanation: "Utiliser des groupes simplifie la gestion : on ajoute/retire des membres du groupe au lieu de modifier les permissions individuellement."
              },
              {
                question: "Que fait GRANT ALL PRIVILEGES ?",
                options: ["Donne uniquement SELECT et MODIFY", "Donne tous les privilèges actuels ET futurs", "Donne les privilèges admin", "Donne l'ownership"],
                correctIndex: 1,
                explanation: "ALL PRIVILEGES inclut tous les privilèges actuels ET ceux qui pourraient être ajoutés dans le futur. À utiliser avec précaution !"
              }
            ]}
          />

          {/* Exercices */}
          <LessonExercises
            lessonSlug="5-2-gestion-permissions"
            exercises={[
              {
                id: "perm-roles",
                title: "Configurer les permissions par rôle",
                description: "Configurez les permissions pour 3 rôles : data_engineer (accès complet), data_analyst (SELECT uniquement sur gold), data_scientist (SELECT sur silver et gold).",
                difficulty: "moyen" as const,
                type: "code" as const,
                prompt: "Écrivez les commandes SQL pour configurer les permissions des 3 rôles sur un catalog 'production' contenant les schemas 'bronze', 'silver' et 'gold'. Chaque rôle doit avoir USAGE aux niveaux nécessaires et les privilèges appropriés.",
                hints: ["Créez d'abord les groupes", "GRANT USAGE à chaque niveau nécessaire", "Le privilege SELECT permet la lecture"],
                solution: {
                  code: `-- Permissions pour data_engineer (accès complet)\nGRANT USAGE ON CATALOG production TO \`data_engineers\`;\nGRANT USAGE ON SCHEMA production.bronze TO \`data_engineers\`;\nGRANT USAGE ON SCHEMA production.silver TO \`data_engineers\`;\nGRANT USAGE ON SCHEMA production.gold TO \`data_engineers\`;\nGRANT ALL PRIVILEGES ON SCHEMA production.bronze TO \`data_engineers\`;\nGRANT ALL PRIVILEGES ON SCHEMA production.silver TO \`data_engineers\`;\nGRANT ALL PRIVILEGES ON SCHEMA production.gold TO \`data_engineers\`;\n\n-- Permissions pour data_analyst (SELECT sur gold uniquement)\nGRANT USAGE ON CATALOG production TO \`data_analysts\`;\nGRANT USAGE ON SCHEMA production.gold TO \`data_analysts\`;\nGRANT SELECT ON SCHEMA production.gold TO \`data_analysts\`;\n\n-- Permissions pour data_scientist (SELECT sur silver et gold)\nGRANT USAGE ON CATALOG production TO \`data_scientists\`;\nGRANT USAGE ON SCHEMA production.silver TO \`data_scientists\`;\nGRANT USAGE ON SCHEMA production.gold TO \`data_scientists\`;\nGRANT SELECT ON SCHEMA production.silver TO \`data_scientists\`;\nGRANT SELECT ON SCHEMA production.gold TO \`data_scientists\`;`,
                  language: "sql",
                  explanation: "Chaque rôle a des accès différents selon le principe du moindre privilège. Les analysts n'ont pas besoin de voir les données brutes Bronze."
                }
              },
              {
                id: "perm-secure-view",
                title: "Créer une vue sécurisée",
                description: "Créez une vue dynamique qui masque les données PII (informations personnelles identifiables) pour les utilisateurs non-admin.",
                difficulty: "difficile" as const,
                type: "code" as const,
                prompt: "Créez une vue 'v_clients_secure' sur la table 'production.gold.clients' (colonnes : id, nom, email, telephone, adresse, date_naissance) qui masque email, telephone et date_naissance pour les utilisateurs qui ne sont pas membres du groupe 'admins'.",
                hints: ["Utilisez is_member() pour vérifier le groupe", "CASE WHEN pour masquer les colonnes sensibles", "current_user() pour la sécurité au niveau ligne"],
                solution: {
                  code: `-- Vue dynamique avec masquage de données PII\nCREATE OR REPLACE VIEW production.gold.v_clients_secure AS\nSELECT\n  id,\n  nom,\n  CASE\n    WHEN is_member('admins') THEN email\n    ELSE CONCAT(LEFT(email, 2), '***@***')\n  END AS email,\n  CASE\n    WHEN is_member('admins') THEN telephone\n    ELSE 'MASQUÉ'\n  END AS telephone,\n  adresse,\n  CASE\n    WHEN is_member('admins') THEN date_naissance\n    ELSE NULL\n  END AS date_naissance\nFROM production.gold.clients;\n\n-- Accorder SELECT sur la vue sécurisée\nGRANT SELECT ON VIEW production.gold.v_clients_secure TO \`data_analysts\`;`,
                  language: "sql",
                  explanation: "Les vues dynamiques sont le mécanisme principal pour le masquage de données et la sécurité fine dans Unity Catalog."
                }
              }
            ]}
          />

          {/* Bouton de complétion */}
          <LessonCompleteButton
            lessonSlug="5-2-gestion-permissions"
          />

          {/* Navigation */}
          <div className="flex justify-between items-center mt-12 pt-8 border-t border-gray-200">
            <Link
              href="/modules/5-1-unity-catalog"
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
              Leçon précédente : Unity Catalog
            </Link>
          </div>
        </div>
      </main>
    </div>
  );
}
