import Link from "next/link";

type ExerciseCard = {
  title: string;
  href: string;
  level: "Débutant" | "Intermédiaire" | "Avancé" | "Tous niveaux";
  duration: string;
  description: string;
  emoji: string;
};

const exercicesModules: ExerciseCard[] = [
  {
    title: "Exercices Module 1-2 : Fondamentaux",
    href: "/exercices/fondamentaux",
    level: "Débutant",
    duration: "3h",
    description:
      "Clusters, notebooks, bases de données, tables, vues, CTEs, MERGE et fonctions SQL avancées.",
    emoji: "🧱",
  },
  {
    title: "Exercices Module 3 : Streaming & Multi-Hop",
    href: "/exercices/streaming-multi-hop",
    level: "Intermédiaire",
    duration: "4h",
    description:
      "Structured Streaming, Auto Loader, architecture Medallion Bronze → Silver → Gold.",
    emoji: "🌊",
  },
  {
    title: "Exercices Module 4-5 : Production & Gouvernance",
    href: "/exercices/production-gouvernance",
    level: "Intermédiaire",
    duration: "4h",
    description:
      "Delta Live Tables, orchestration Jobs, Unity Catalog et gestion des permissions.",
    emoji: "🏗️",
  },
  {
    title: "Delta Lake Avancé",
    href: "/exercices/delta-lake-avance",
    level: "Avancé",
    duration: "3h",
    description:
      "Time Travel, OPTIMIZE, Z-ORDER, VACUUM, Change Data Feed et optimisations avancées.",
    emoji: "⚡",
  },
  {
    title: "Quiz de Certification",
    href: "/exercices/quiz-certification",
    level: "Tous niveaux",
    duration: "2h",
    description:
      "QCM d'entraînement couvrant tous les modules pour préparer la certification Databricks.",
    emoji: "📝",
  },
];

const miniProjets: ExerciseCard[] = [
  {
    title: "Projet E-commerce",
    href: "/exercices/projet-ecommerce",
    level: "Intermédiaire",
    duration: "6h",
    description:
      "Pipeline complet d'ingestion de données e-commerce : commandes, clients, produits — de Bronze à Gold.",
    emoji: "🛒",
  },
  {
    title: "Projet IoT Streaming",
    href: "/exercices/projet-iot",
    level: "Avancé",
    duration: "6h",
    description:
      "Pipeline streaming temps réel avec capteurs IoT, détection d'anomalies et agrégations par fenêtre.",
    emoji: "📡",
  },
];

const projetFinal: ExerciseCard[] = [
  {
    title: "Projet Final : Cas SNCF",
    href: "/exercices/projet-final",
    level: "Avancé",
    duration: "14h",
    description:
      "Projet complet simulant un cas réel SNCF : ingestion, pipeline Medallion, DLT, Unity Catalog, orchestration et monitoring.",
    emoji: "🚄",
  },
  {
    title: "Préparation Entretiens",
    href: "/exercices/preparation-entretiens",
    level: "Tous niveaux",
    duration: "5h",
    description:
      "Questions techniques fréquentes, conseils pour les entretiens data engineer et portfolio.",
    emoji: "🎯",
  },
];

function LevelBadge({ level }: { level: ExerciseCard["level"] }) {
  const colors: Record<string, string> = {
    Débutant: "bg-green-100 text-green-800 border-green-300",
    Intermédiaire: "bg-amber-100 text-amber-800 border-amber-300",
    Avancé: "bg-red-100 text-red-800 border-red-300",
    "Tous niveaux": "bg-purple-100 text-purple-800 border-purple-300",
  };
  return (
    <span
      className={`text-xs font-semibold px-2.5 py-0.5 rounded-full border ${colors[level]}`}
    >
      {level}
    </span>
  );
}

function ExerciseCardComponent({ card }: { card: ExerciseCard }) {
  return (
    <Link
      href={card.href}
      className="block bg-white rounded-xl border border-gray-200 shadow-sm hover:shadow-lg hover:-translate-y-1 transition-all duration-200 overflow-hidden group"
    >
      <div className="px-5 py-5 space-y-3">
        <div className="flex items-start justify-between gap-3">
          <div className="flex items-center gap-3">
            <span className="text-2xl">{card.emoji}</span>
            <h3 className="text-base font-bold text-[#1b3a4b] group-hover:text-[#ff3621] transition-colors">
              {card.title}
            </h3>
          </div>
        </div>
        <p className="text-sm text-gray-600 leading-relaxed">
          {card.description}
        </p>
        <div className="flex items-center gap-3 pt-1">
          <LevelBadge level={card.level} />
          <span className="text-xs font-medium bg-[#1b3a4b]/10 text-[#1b3a4b] px-2.5 py-1 rounded-full">
            ⏱ {card.duration}
          </span>
        </div>
      </div>
    </Link>
  );
}

function SectionTitle({
  emoji,
  title,
}: {
  emoji: string;
  title: string;
}) {
  return (
    <div className="flex items-center gap-3 mb-6 mt-12">
      <span className="text-2xl">{emoji}</span>
      <h2 className="text-2xl font-bold text-[#1b3a4b]">{title}</h2>
    </div>
  );
}

export default function ExercicesPage() {
  return (
    <div className="min-h-[calc(100vh-4rem)]">
      {/* Hero */}
      <div className="relative bg-gradient-to-br from-[#1b3a4b] via-[#2d5f7a] to-[#1b3a4b] text-white overflow-hidden">
        <div className="absolute inset-0 opacity-10">
          <div className="absolute top-10 left-10 w-72 h-72 bg-[#ff3621] rounded-full blur-3xl" />
          <div className="absolute bottom-10 right-10 w-96 h-96 bg-blue-400 rounded-full blur-3xl" />
        </div>
        <div className="relative max-w-6xl mx-auto px-6 py-16 lg:py-20 text-center">
          <span className="inline-block text-5xl mb-4">🏋️</span>
          <h1 className="text-4xl lg:text-5xl font-extrabold mb-4">
            Exercices &amp; Projets Pratiques
          </h1>
          <p className="text-lg lg:text-xl text-white/80 max-w-2xl mx-auto leading-relaxed">
            Mettez en pratique vos connaissances Databricks avec des exercices
            progressifs, des mini-projets et un projet final complet.
          </p>
          <div className="flex flex-wrap justify-center gap-4 mt-8">
            <div className="bg-white/10 backdrop-blur-sm rounded-lg px-4 py-2 text-sm">
              📚 9 exercices &amp; projets
            </div>
            <div className="bg-white/10 backdrop-blur-sm rounded-lg px-4 py-2 text-sm">
              ⏱ ~47 heures de pratique
            </div>
            <div className="bg-white/10 backdrop-blur-sm rounded-lg px-4 py-2 text-sm">
              🎯 Du débutant à l&apos;avancé
            </div>
          </div>
        </div>
      </div>

      {/* Content */}
      <div className="max-w-6xl mx-auto px-6 py-10">
        {/* Back to programme */}
        <Link
          href="/programme"
          className="inline-flex items-center gap-2 text-sm text-[#1b3a4b] hover:text-[#ff3621] transition-colors mb-6"
        >
          <span>←</span> Retour au programme
        </Link>

        {/* Exercices par module */}
        <SectionTitle emoji="📘" title="Exercices par Module" />
        <div className="grid gap-5 sm:grid-cols-2 lg:grid-cols-3">
          {exercicesModules.map((card) => (
            <ExerciseCardComponent key={card.href} card={card} />
          ))}
        </div>

        {/* Mini-Projets */}
        <SectionTitle emoji="🔨" title="Mini-Projets" />
        <div className="grid gap-5 sm:grid-cols-2">
          {miniProjets.map((card) => (
            <ExerciseCardComponent key={card.href} card={card} />
          ))}
        </div>

        {/* Projet Final */}
        <SectionTitle emoji="🏆" title="Projet Final &amp; Préparation" />
        <div className="grid gap-5 sm:grid-cols-2">
          {projetFinal.map((card) => (
            <ExerciseCardComponent key={card.href} card={card} />
          ))}
        </div>

        {/* Conseil */}
        <div className="mt-12 bg-gradient-to-r from-[#1b3a4b]/5 to-[#ff3621]/5 rounded-xl border border-gray-200 p-6">
          <h3 className="text-lg font-bold text-[#1b3a4b] mb-2">
            💡 Conseil de progression
          </h3>
          <p className="text-sm text-gray-700 leading-relaxed">
            Suivez les exercices dans l&apos;ordre pour une progression
            optimale. Chaque module s&apos;appuie sur les connaissances
            acquises dans les précédents. N&apos;hésitez pas à revenir sur les
            cours si un concept n&apos;est pas clair avant de passer à
            l&apos;exercice suivant.
          </p>
        </div>
      </div>
    </div>
  );
}
