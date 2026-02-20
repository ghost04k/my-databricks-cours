"use client";

import Link from "next/link";
import Sidebar from "@/components/Sidebar";
import CodeBlock from "@/components/CodeBlock";
import InfoBox from "@/components/InfoBox";
import Quiz from "@/components/Quiz";
import LessonExercises from "@/components/LessonExercises";
import LessonCompleteButton from "@/components/LessonCompleteButton";

export default function BasesNotebooksPage() {
  return (
    <div className="flex min-h-[calc(100vh-4rem)]">
      <Sidebar currentPath="/modules/1-1-bases-notebooks" />

      <main className="flex-1 overflow-y-auto">
        <div className="max-w-4xl mx-auto px-6 py-10 lg:px-10">
          {/* Header */}
          <div className="mb-10">
            <div className="flex items-center gap-3 mb-3">
              <span className="inline-flex items-center px-3 py-1 rounded-full text-xs font-semibold bg-blue-100 text-blue-800">
                Module 1
              </span>
              <span className="text-sm text-[var(--color-text-light)]">
                Leçon 1.1
              </span>
            </div>
            <h1 className="text-3xl font-bold text-[var(--color-text)] mb-3">
              Bases des Notebooks
            </h1>
            <p className="text-lg text-[var(--color-text-light)] leading-relaxed">
              Découvrez les notebooks Databricks : les commandes magiques, le
              formatage Markdown, les utilitaires dbutils et le partage de
              variables entre notebooks.
            </p>
          </div>

          {/* Content */}
          <section className="space-y-8">
            {/* Qu'est-ce qu'un notebook ? */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Qu&apos;est-ce qu&apos;un Notebook Databricks ?
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Un <strong>notebook Databricks</strong> est un document
                interactif composé de cellules pouvant contenir du code, du texte
                formaté ou des visualisations. Il fonctionne de manière
                similaire à un Jupyter Notebook, mais avec des fonctionnalités
                supplémentaires propres à Databricks.
              </p>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Chaque notebook est associé à un langage par défaut (Python, SQL,
                Scala ou R), mais vous pouvez mélanger les langages dans un même
                notebook grâce aux <strong>commandes magiques</strong>.
              </p>
            </div>

            <InfoBox type="warning" title="Attachez un cluster avant d'exécuter">
              Avant de pouvoir exécuter une cellule de code, vous devez
              impérativement <strong>attacher un cluster</strong> à votre
              notebook. Cliquez sur le menu déroulant en haut à gauche du
              notebook et sélectionnez votre cluster (par exemple,{" "}
              <strong>Demo Cluster</strong>). Si le cluster est éteint, il sera
              redémarré automatiquement.
            </InfoBox>

            {/* Commandes magiques */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Les Commandes Magiques
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Les commandes magiques permettent de changer le langage
                d&apos;exécution d&apos;une cellule, indépendamment du langage
                par défaut du notebook. Elles commencent par le symbole{" "}
                <code className="bg-gray-100 px-2 py-0.5 rounded text-sm">%</code>{" "}
                et doivent être placées en <strong>première ligne</strong> de la
                cellule.
              </p>

              <div className="overflow-x-auto mb-6">
                <table className="w-full text-sm border-collapse">
                  <thead>
                    <tr className="bg-gray-50">
                      <th className="text-left px-4 py-3 border border-gray-200 font-semibold text-[var(--color-text)]">
                        Commande
                      </th>
                      <th className="text-left px-4 py-3 border border-gray-200 font-semibold text-[var(--color-text)]">
                        Description
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr>
                      <td className="px-4 py-3 border border-gray-200 font-mono text-sm">
                        %python
                      </td>
                      <td className="px-4 py-3 border border-gray-200 text-[var(--color-text-light)]">
                        Exécute la cellule en Python
                      </td>
                    </tr>
                    <tr className="bg-gray-50">
                      <td className="px-4 py-3 border border-gray-200 font-mono text-sm">
                        %sql
                      </td>
                      <td className="px-4 py-3 border border-gray-200 text-[var(--color-text-light)]">
                        Exécute la cellule en SQL
                      </td>
                    </tr>
                    <tr>
                      <td className="px-4 py-3 border border-gray-200 font-mono text-sm">
                        %md
                      </td>
                      <td className="px-4 py-3 border border-gray-200 text-[var(--color-text-light)]">
                        Affiche du texte formaté en Markdown
                      </td>
                    </tr>
                    <tr className="bg-gray-50">
                      <td className="px-4 py-3 border border-gray-200 font-mono text-sm">
                        %fs
                      </td>
                      <td className="px-4 py-3 border border-gray-200 text-[var(--color-text-light)]">
                        Exécute des commandes du système de fichiers DBFS
                      </td>
                    </tr>
                    <tr>
                      <td className="px-4 py-3 border border-gray-200 font-mono text-sm">
                        %run
                      </td>
                      <td className="px-4 py-3 border border-gray-200 text-[var(--color-text-light)]">
                        Exécute un autre notebook depuis le notebook courant
                      </td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </div>

            {/* Exemples Python */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Exemples de Code
              </h2>

              <h3 className="text-xl font-medium text-[var(--color-text)] mb-3">
                Python
              </h3>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                Dans un notebook Python ou en utilisant la commande magique{" "}
                <code className="bg-gray-100 px-2 py-0.5 rounded text-sm">%python</code>,
                vous pouvez exécuter du code Python standard :
              </p>
              <CodeBlock
                language="python"
                title="Hello World en Python"
                code={`%python
print("Hello World!")

# Vous pouvez aussi utiliser les fonctionnalités Spark
print(f"Version de Spark : {spark.version}")`}
              />

              <h3 className="text-xl font-medium text-[var(--color-text)] mb-3 mt-6">
                SQL
              </h3>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                La commande magique{" "}
                <code className="bg-gray-100 px-2 py-0.5 rounded text-sm">%sql</code>{" "}
                permet d&apos;écrire des requêtes SQL directement dans une
                cellule :
              </p>
              <CodeBlock
                language="sql"
                title="Hello World en SQL"
                code={`%sql
SELECT "Hello world from SQL!" AS message;

-- Afficher la date actuelle
SELECT current_date() AS date_du_jour;`}
              />
            </div>

            {/* Markdown */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Formatage Markdown
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                La commande magique{" "}
                <code className="bg-gray-100 px-2 py-0.5 rounded text-sm">%md</code>{" "}
                permet de créer du contenu enrichi directement dans vos
                notebooks. Voici les principales syntaxes disponibles :
              </p>
              <CodeBlock
                language="python"
                title="Syntaxe Markdown dans Databricks"
                code={`%md
# Titre de niveau 1
## Titre de niveau 2
### Titre de niveau 3

**Texte en gras** et *texte en italique*

- Élément de liste 1
- Élément de liste 2
  - Sous-élément

1. Liste numérotée
2. Deuxième élément

[Lien vers Databricks](https://databricks.com)

![Image](https://url-de-image.png)

| Colonne 1 | Colonne 2 |
|-----------|-----------|
| Valeur A  | Valeur B  |
| Valeur C  | Valeur D  |`}
              />
            </div>

            {/* %run */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Inclure d&apos;autres Notebooks avec %run
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                La commande{" "}
                <code className="bg-gray-100 px-2 py-0.5 rounded text-sm">%run</code>{" "}
                permet d&apos;exécuter un autre notebook depuis le notebook
                courant. Toutes les variables et fonctions définies dans le
                notebook appelé deviennent disponibles dans le notebook
                appelant.
              </p>
              <CodeBlock
                language="python"
                title="Utilisation de %run"
                code={`%run ./Includes/Setup

# Après l'exécution, les variables du notebook "Setup"
# sont disponibles dans ce notebook
print(f"Dataset path: {dataset_path}")
print(f"Database name: {database_name}")`}
              />

              <InfoBox type="info" title="Comportement de %run">
                La commande <strong>%run</strong> exécute le notebook cible dans
                le <strong>même contexte d&apos;exécution</strong> que le
                notebook appelant. Cela signifie que les variables, fonctions et
                imports définis dans le notebook cible sont directement
                accessibles. Attention : <strong>%run</strong> doit être la seule
                instruction dans la cellule, vous ne pouvez pas ajouter d&apos;autre
                code dans la même cellule.
              </InfoBox>
            </div>

            {/* dbutils */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Les Utilitaires dbutils
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                <code className="bg-gray-100 px-2 py-0.5 rounded text-sm">dbutils</code>{" "}
                est une bibliothèque intégrée à Databricks qui fournit des
                utilitaires pour interagir avec le système de fichiers, gérer
                les secrets, les widgets et bien plus encore.
              </p>

              <CodeBlock
                language="python"
                title="Aide dbutils"
                code={`# Afficher l'aide générale de dbutils
dbutils.help()

# Afficher l'aide sur les commandes du système de fichiers
dbutils.fs.help()`}
              />

              <CodeBlock
                language="python"
                title="Lister les fichiers avec dbutils.fs"
                code={`# Lister les fichiers à la racine de DBFS
dbutils.fs.ls("/")

# Lister les fichiers dans un répertoire spécifique
dbutils.fs.ls("/databricks-datasets/")

# Afficher les premiers fichiers d'un dataset
files = dbutils.fs.ls("/databricks-datasets/")
for f in files[:5]:
    print(f"Nom: {f.name}, Taille: {f.size}, Chemin: {f.path}")`}
              />

              <p className="text-[var(--color-text-light)] leading-relaxed mb-4 mt-4">
                Vous pouvez aussi utiliser la commande magique{" "}
                <code className="bg-gray-100 px-2 py-0.5 rounded text-sm">%fs</code>{" "}
                comme raccourci pour{" "}
                <code className="bg-gray-100 px-2 py-0.5 rounded text-sm">dbutils.fs</code> :
              </p>

              <CodeBlock
                language="python"
                title="Raccourci %fs"
                code={`%fs ls /databricks-datasets/

-- Équivalent à :
-- dbutils.fs.ls("/databricks-datasets/")`}
              />
            </div>

            {/* Partage de variables */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Partage de Variables entre Notebooks
              </h2>
              <p className="text-[var(--color-text-light)] leading-relaxed mb-4">
                En plus de <strong>%run</strong>, vous pouvez partager des
                données entre notebooks en utilisant des tables temporaires ou
                les widgets de dbutils :
              </p>

              <CodeBlock
                language="python"
                title="Notebook A : Définir des variables"
                code={`# Avec %run, les variables sont automatiquement partagées
username = "admin"
database_name = f"db_{username}"
dataset_path = f"/datasets/{username}"

print(f"Configuration chargée pour {username}")`}
              />

              <CodeBlock
                language="python"
                title="Notebook B : Utiliser les variables"
                code={`# Exécuter le notebook A pour récupérer ses variables
%run ./Notebook_A

# Les variables sont maintenant disponibles
print(f"Utilisateur : {username}")
print(f"Base de données : {database_name}")
print(f"Chemin des données : {dataset_path}")`}
              />

              <CodeBlock
                language="python"
                title="Utilisation des widgets dbutils"
                code={`# Créer un widget de texte
dbutils.widgets.text("nom", "valeur_defaut", "Label du widget")

# Récupérer la valeur d'un widget
nom = dbutils.widgets.get("nom")
print(f"Valeur du widget : {nom}")

# Supprimer un widget
dbutils.widgets.remove("nom")

# Supprimer tous les widgets
dbutils.widgets.removeAll()`}
              />
            </div>

            {/* Raccourcis clavier */}
            <div>
              <h2 className="text-2xl font-semibold text-[var(--color-text)] mb-4">
                Raccourcis Clavier Utiles
              </h2>
              <div className="overflow-x-auto">
                <table className="w-full text-sm border-collapse">
                  <thead>
                    <tr className="bg-gray-50">
                      <th className="text-left px-4 py-3 border border-gray-200 font-semibold text-[var(--color-text)]">
                        Raccourci
                      </th>
                      <th className="text-left px-4 py-3 border border-gray-200 font-semibold text-[var(--color-text)]">
                        Action
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr>
                      <td className="px-4 py-3 border border-gray-200 font-mono text-sm">
                        Shift + Enter
                      </td>
                      <td className="px-4 py-3 border border-gray-200 text-[var(--color-text-light)]">
                        Exécuter la cellule et passer à la suivante
                      </td>
                    </tr>
                    <tr className="bg-gray-50">
                      <td className="px-4 py-3 border border-gray-200 font-mono text-sm">
                        Ctrl + Enter
                      </td>
                      <td className="px-4 py-3 border border-gray-200 text-[var(--color-text-light)]">
                        Exécuter la cellule sans se déplacer
                      </td>
                    </tr>
                    <tr>
                      <td className="px-4 py-3 border border-gray-200 font-mono text-sm">
                        Ctrl + Shift + Enter
                      </td>
                      <td className="px-4 py-3 border border-gray-200 text-[var(--color-text-light)]">
                        Exécuter toutes les cellules du notebook
                      </td>
                    </tr>
                    <tr className="bg-gray-50">
                      <td className="px-4 py-3 border border-gray-200 font-mono text-sm">
                        Esc + A
                      </td>
                      <td className="px-4 py-3 border border-gray-200 text-[var(--color-text-light)]">
                        Insérer une cellule au-dessus
                      </td>
                    </tr>
                    <tr>
                      <td className="px-4 py-3 border border-gray-200 font-mono text-sm">
                        Esc + B
                      </td>
                      <td className="px-4 py-3 border border-gray-200 text-[var(--color-text-light)]">
                        Insérer une cellule en dessous
                      </td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </div>

            {/* InfoBox tip */}
            <InfoBox type="tip" title="Astuce : Organiser vos notebooks">
              Utilisez les cellules Markdown (<strong>%md</strong>) pour
              structurer vos notebooks avec des titres, des explications et de
              la documentation. Un notebook bien documenté est plus facile à
              comprendre et à maintenir, surtout lorsque vous travaillez en
              équipe.
            </InfoBox>
          </section>

          {/* Quiz & Exercises */}
          <Quiz
            lessonSlug="1-1-bases-notebooks"
            title="Quiz : Bases des Notebooks"
            questions={[
              {
                question: "Quelle commande magique permet d'exécuter du SQL dans une cellule Python ?",
                options: ["%python", "%sql", "%run", "%md"],
                correctIndex: 1,
                explanation: "%sql permet d'exécuter des requêtes SQL dans un notebook dont le langage par défaut est Python."
              },
              {
                question: "Que fait la commande %run ?",
                options: [
                  "Elle exécute un script Python externe",
                  "Elle exécute un autre notebook et importe ses variables/fonctions dans le contexte courant",
                  "Elle redémarre le cluster",
                  "Elle exécute toutes les cellules du notebook"
                ],
                correctIndex: 1,
                explanation: "%run exécute un autre notebook comme s'il faisait partie du notebook courant, rendant disponibles ses variables et fonctions."
              },
              {
                question: "Comment lister les fichiers dans DBFS avec dbutils ?",
                options: [
                  "dbutils.fs.list('/path')",
                  "dbutils.fs.ls('/path')",
                  "dbutils.files.list('/path')",
                  "dbutils.ls('/path')"
                ],
                correctIndex: 1,
                explanation: "dbutils.fs.ls() liste les fichiers et répertoires dans le système de fichiers Databricks (DBFS)."
              },
              {
                question: "Que se passe-t-il si vous exécutez une cellule sans cluster attaché ?",
                options: [
                  "La cellule s'exécute localement",
                  "Un cluster par défaut est créé automatiquement",
                  "Vous obtenez une erreur, il faut d'abord attacher un cluster",
                  "Seules les cellules Markdown fonctionnent"
                ],
                correctIndex: 2,
                explanation: "Un cluster doit être attaché et démarré pour exécuter du code. Sans cluster, une erreur est affichée."
              },
              {
                question: "Quelle est la différence entre %md et un commentaire Python ?",
                options: [
                  "Aucune différence",
                  "%md génère du texte formaté en Markdown visible dans le notebook, un commentaire est ignoré",
                  "%md est plus rapide",
                  "Les commentaires supportent aussi le Markdown"
                ],
                correctIndex: 1,
                explanation: "%md rend du texte en Markdown (titres, listes, images, tableaux) directement dans le notebook, ce qui est parfait pour la documentation."
              }
            ]}
          />

          <LessonExercises
            lessonSlug="1-1-bases-notebooks"
            exercises={[
              {
                id: "ex1",
                title: "Créer un notebook multi-langage",
                description: "Pratiquer les commandes magiques",
                difficulty: "facile",
                type: "code",
                prompt: "Créez un notebook avec 4 cellules :\n1. Une cellule Markdown avec un titre et une description\n2. Une cellule Python qui crée une variable 'nom = \"Databricks\"'\n3. Une cellule SQL qui sélectionne la valeur 42\n4. Une cellule Python qui utilise dbutils.fs.ls('/') pour lister les fichiers",
                hints: [
                  "Utilisez %md pour le Markdown",
                  "En Python, pas besoin de commande magique si c'est le langage par défaut",
                  "Utilisez %sql pour la cellule SQL"
                ],
                solution: {
                  code: "# Cellule 1 :\n# %md\n# # Mon Premier Notebook\n# Ce notebook explore les bases de Databricks.\n\n# Cellule 2 :\nnom = \"Databricks\"\nprint(f\"Bienvenue sur {nom} !\")\n\n# Cellule 3 :\n# %sql\n# SELECT 42 AS ma_valeur;\n\n# Cellule 4 :\nfiles = dbutils.fs.ls('/')\ndisplay(files)",
                  language: "python",
                  explanation: "Chaque cellule utilise la commande magique appropriée. La cellule Markdown s'affiche en texte formaté, les cellules Python et SQL exécutent du code."
                }
              },
              {
                id: "ex2",
                title: "Explorer dbutils",
                description: "Découvrir les commandes utilitaires",
                difficulty: "moyen",
                type: "code",
                prompt: "Utilisez dbutils pour :\n1. Lister les sous-commandes disponibles avec dbutils.help()\n2. Lister les méthodes de dbutils.fs avec dbutils.fs.help()\n3. Créer un répertoire /tmp/mon_test avec dbutils.fs.mkdirs()\n4. Vérifier que le répertoire existe avec dbutils.fs.ls()",
                hints: [
                  "dbutils.help() affiche toutes les catégories",
                  "dbutils.fs.mkdirs() crée un répertoire récursivement",
                  "Utilisez display() pour un affichage plus lisible"
                ],
                solution: {
                  code: "# 1. Aide générale\ndbutils.help()\n\n# 2. Aide sur le filesystem\ndbutils.fs.help()\n\n# 3. Créer un répertoire\ndbutils.fs.mkdirs('/tmp/mon_test')\n\n# 4. Vérifier\ndisplay(dbutils.fs.ls('/tmp/'))",
                  language: "python",
                  explanation: "dbutils fournit des utilitaires pour le filesystem (fs), les secrets, les widgets et les notebooks. C'est un outil essentiel pour interagir avec l'environnement Databricks."
                }
              }
            ]}
          />

          <LessonCompleteButton lessonSlug="1-1-bases-notebooks" />

          {/* Navigation */}
          <div className="flex items-center justify-between mt-12 pt-6 border-t border-gray-200">
            <Link
              href="/modules/1-0-creation-clusters"
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
              Leçon précédente : Création de Clusters
            </Link>
            <Link
              href="/modules/2-1-bases-donnees-tables"
              className="inline-flex items-center gap-2 px-5 py-2.5 bg-[#ff3621] text-white rounded-lg font-medium hover:bg-[#e02e1a] transition-colors"
            >
              Leçon suivante : Bases de données et Tables
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
