import Link from "next/link";

type DayCard = {
  day: number;
  title: string;
  duration: string;
  level: "Débutant" | "Intermédiaire" | "Avancé";
  morning?: string;
  afternoon?: string;
  exercise?: string;
  bullets?: string[];
  links: { label: string; href: string }[];
};

const week1Days: DayCard[] = [
  {
    day: 1,
    title: "Découverte de Databricks",
    duration: "4h",
    level: "Débutant",
    morning:
      "Comprendre l'architecture Lakehouse, créer un compte Databricks Community Edition",
    afternoon: "Créer et configurer un cluster, explorer l'interface",
    exercise:
      "Créer un cluster, lancer un notebook, exécuter des commandes basiques",
    links: [
      { label: "Création de clusters", href: "/modules/1-0-creation-clusters" },
      { label: "Bases des notebooks", href: "/modules/1-1-bases-notebooks" },
    ],
  },
  {
    day: 2,
    title: "SQL sur Databricks",
    duration: "5h",
    level: "Débutant",
    morning:
      "Bases de données, tables managées vs externes, DESCRIBE EXTENDED",
    afternoon:
      "Vues (stored, temp, global temp), CTEs, requêtes complexes",
    exercise:
      "Créer une base, des tables, des vues, pratiquer les CTEs",
    links: [
      {
        label: "Bases de données & tables",
        href: "/modules/2-1-bases-donnees-tables",
      },
      { label: "Vues & CTEs", href: "/modules/2-2-vues-ctes" },
    ],
  },
  {
    day: 3,
    title: "Transformations de données",
    duration: "5h",
    level: "Intermédiaire",
    morning:
      "INSERT INTO, INSERT OVERWRITE, MERGE INTO pour les upserts",
    afternoon:
      "COPY INTO, déduplication, fonctions avancées (FILTER, TRANSFORM, EXISTS)",
    exercise: "Pipeline ETL simple avec MERGE INTO et déduplication",
    links: [
      {
        label: "Transformations",
        href: "/modules/2-3-transformations-donnees",
      },
      {
        label: "Fonctions SQL avancées",
        href: "/modules/2-4-fonctions-sql-avancees",
      },
    ],
  },
  {
    day: 4,
    title: "Structured Streaming",
    duration: "5h",
    level: "Intermédiaire",
    morning:
      "Concepts du streaming, readStream/writeStream, triggers, output modes",
    afternoon: "Checkpointing, fenêtres de temps, watermarks",
    exercise:
      "Créer un pipeline streaming qui lit des fichiers JSON et écrit dans une table Delta",
    links: [
      {
        label: "Structured Streaming",
        href: "/modules/3-1-structured-streaming",
      },
    ],
  },
  {
    day: 5,
    title: "Auto Loader & Multi-Hop",
    duration: "5h",
    level: "Intermédiaire",
    morning:
      "Auto Loader (cloudFiles), inférence de schéma, _rescued_data",
    afternoon:
      "Architecture Medallion (Bronze → Silver → Gold), pipeline complet",
    exercise: "Construire un pipeline complet Bronze → Silver → Gold",
    links: [
      { label: "Auto Loader", href: "/modules/3-2-auto-loader" },
      {
        label: "Architecture Multi-Hop",
        href: "/modules/3-3-architecture-multi-hop",
      },
    ],
  },
];

const week2Days: DayCard[] = [
  {
    day: 6,
    title: "Delta Live Tables",
    duration: "6h",
    level: "Intermédiaire",
    morning:
      "Syntaxe DLT (SQL et Python), Streaming Live Tables vs Live Tables",
    afternoon:
      "Expectations (qualité des données), modes triggered vs continuous",
    exercise: "Créer un pipeline DLT complet avec expectations",
    links: [
      { label: "Delta Live Tables", href: "/modules/4-1-delta-live-tables" },
    ],
  },
  {
    day: 7,
    title: "Orchestration & Monitoring",
    duration: "5h",
    level: "Intermédiaire",
    morning:
      "Event log DLT, monitoring des pipelines, métriques de qualité",
    afternoon:
      "Jobs multi-tâches, scheduling, task values, retry policies",
    exercise: "Créer un job multi-tâches avec dépendances",
    links: [
      {
        label: "Résultats Pipeline",
        href: "/modules/4-2-resultats-pipeline",
      },
      {
        label: "Orchestration Jobs",
        href: "/modules/4-3-orchestration-jobs",
      },
    ],
  },
  {
    day: 8,
    title: "Gouvernance avec Unity Catalog",
    duration: "5h",
    level: "Intermédiaire",
    morning: "Namespace 3 niveaux, catalogues, schémas",
    afternoon:
      "GRANT/REVOKE, vues dynamiques, sécurité row-level/column-level",
    exercise: "Configurer Unity Catalog avec permissions granulaires",
    links: [
      { label: "Unity Catalog", href: "/modules/5-1-unity-catalog" },
      {
        label: "Gestion des permissions",
        href: "/modules/5-2-gestion-permissions",
      },
    ],
  },
  {
    day: 9,
    title: "Mini-Projet : Pipeline E-commerce",
    duration: "6h",
    level: "Avancé",
    bullets: [
      "Contexte : Données de commandes e-commerce (JSON), clients (CSV), produits (Parquet)",
      "Objectif : Pipeline complet d'ingestion à analytics",
      "Bronze : Ingestion avec Auto Loader",
      "Silver : Nettoyage, jointures, déduplication",
      "Gold : KPIs (CA par catégorie, meilleurs clients, tendances)",
    ],
    links: [
      { label: "Projet E-commerce", href: "/exercices/projet-ecommerce" },
    ],
  },
  {
    day: 10,
    title: "Mini-Projet : Pipeline IoT",
    duration: "6h",
    level: "Avancé",
    bullets: [
      "Contexte : Capteurs IoT envoyant des données de température/pression toutes les secondes",
      "Objectif : Streaming pipeline avec alertes",
      "Bronze : Ingestion streaming des données capteurs",
      "Silver : Filtrage des anomalies, enrichissement",
      "Gold : Agrégations par fenêtre de temps, dashboard",
    ],
    links: [{ label: "Projet IoT", href: "/exercices/projet-iot" }],
  },
];

const week3Days: DayCard[] = [
  {
    day: 11,
    title: "Deep Dive Delta Lake",
    duration: "5h",
    level: "Avancé",
    bullets: [
      "Time Travel (RESTORE, VERSION AS OF)",
      "Optimisation (OPTIMIZE, Z-ORDER, VACUUM)",
      "Change Data Feed (CDF)",
      "Exercice pratique sur l'optimisation Delta",
    ],
    links: [
      { label: "Delta Lake avancé", href: "/exercices/delta-lake-avance" },
    ],
  },
  {
    day: 12,
    title: "Préparation Certification",
    duration: "5h",
    level: "Avancé",
    bullets: [
      "Révision de tous les modules",
      "QCM d'entraînement (60 questions type examen)",
      "Points clés à retenir par module",
    ],
    links: [
      { label: "Quiz Certification", href: "/exercices/quiz-certification" },
    ],
  },
  {
    day: 13,
    title: "Projet Final — Partie 1",
    duration: "7h",
    level: "Avancé",
    bullets: [
      "Contexte : Simuler un cas SNCF — données de trafic ferroviaire",
      "Ingestion de données (horaires, retards, gares, voyageurs)",
      "Pipeline Medallion complet avec DLT",
    ],
    links: [{ label: "Projet Final", href: "/exercices/projet-final" }],
  },
  {
    day: 14,
    title: "Projet Final — Partie 2",
    duration: "7h",
    level: "Avancé",
    bullets: [
      "Suite du projet SNCF",
      "Gouvernance avec Unity Catalog",
      "Orchestration complète avec Jobs",
      "Monitoring et qualité des données",
    ],
    links: [{ label: "Projet Final", href: "/exercices/projet-final" }],
  },
  {
    day: 15,
    title: "Portfolio & Préparation Entretiens",
    duration: "5h",
    level: "Avancé",
    bullets: [
      "Documenter le projet final",
      "Préparer les réponses aux questions d'entretien",
      "Questions techniques fréquentes",
      "Conseils pour les entretiens chez les grandes entreprises",
    ],
    links: [
      {
        label: "Préparation Entretiens",
        href: "/exercices/preparation-entretiens",
      },
    ],
  },
];

function LevelBadge({ level }: { level: DayCard["level"] }) {
  const colors: Record<string, string> = {
    Débutant: "bg-green-100 text-green-800 border-green-300",
    Intermédiaire: "bg-amber-100 text-amber-800 border-amber-300",
    Avancé: "bg-red-100 text-red-800 border-red-300",
  };
  return (
    <span
      className={`text-xs font-semibold px-2.5 py-0.5 rounded-full border ${colors[level]}`}
    >
      {level}
    </span>
  );
}

function DayCardComponent({ card }: { card: DayCard }) {
  return (
    <div className="relative pl-8 pb-10 group">
      {/* Timeline line */}
      <div className="absolute left-[15px] top-0 bottom-0 w-0.5 bg-[#1b3a4b]/20 group-last:hidden" />
      {/* Day badge */}
      <div className="absolute left-0 top-0 w-[31px] h-[31px] rounded-full bg-[#1b3a4b] text-white flex items-center justify-center text-xs font-bold shadow-lg z-10">
        {card.day}
      </div>

      <div className="ml-4 bg-white rounded-xl border border-gray-200 shadow-sm hover:shadow-md transition-shadow overflow-hidden">
        {/* Header */}
        <div className="px-5 py-4 border-b border-gray-100 flex flex-col sm:flex-row sm:items-center sm:justify-between gap-2">
          <div>
            <h4 className="text-lg font-bold text-[#1b3a4b]">
              Jour {card.day} — {card.title}
            </h4>
          </div>
          <div className="flex items-center gap-3">
            <span className="text-xs font-medium bg-[#1b3a4b]/10 text-[#1b3a4b] px-2.5 py-1 rounded-full">
              ⏱ {card.duration}
            </span>
            <LevelBadge level={card.level} />
          </div>
        </div>

        {/* Body */}
        <div className="px-5 py-4 space-y-3 text-sm text-gray-700">
          {card.morning && (
            <div className="flex gap-2">
              <span className="text-amber-500 mt-0.5">☀️</span>
              <div>
                <span className="font-semibold text-gray-900">Matin : </span>
                {card.morning}
              </div>
            </div>
          )}
          {card.afternoon && (
            <div className="flex gap-2">
              <span className="text-blue-500 mt-0.5">🌙</span>
              <div>
                <span className="font-semibold text-gray-900">
                  Après-midi :{" "}
                </span>
                {card.afternoon}
              </div>
            </div>
          )}
          {card.exercise && (
            <div className="flex gap-2">
              <span className="text-green-500 mt-0.5">💻</span>
              <div>
                <span className="font-semibold text-gray-900">
                  Exercice :{" "}
                </span>
                {card.exercise}
              </div>
            </div>
          )}
          {card.bullets && (
            <ul className="space-y-1.5 ml-1">
              {card.bullets.map((b, i) => (
                <li key={i} className="flex gap-2">
                  <span className="text-[#ff3621] mt-1 text-xs">▸</span>
                  <span>{b}</span>
                </li>
              ))}
            </ul>
          )}
        </div>

        {/* Links */}
        <div className="px-5 py-3 bg-gray-50 border-t border-gray-100 flex flex-wrap gap-2">
          {card.links.map((link, i) => (
            <Link
              key={i}
              href={link.href}
              className="inline-flex items-center gap-1 text-xs font-medium text-[#1b3a4b] bg-[#1b3a4b]/5 hover:bg-[#1b3a4b]/15 px-3 py-1.5 rounded-lg transition-colors"
            >
              📘 {link.label}
            </Link>
          ))}
        </div>
      </div>
    </div>
  );
}

function WeekSection({
  weekNumber,
  title,
  subtitle,
  days,
  borderColor,
}: {
  weekNumber: number;
  title: string;
  subtitle: string;
  days: DayCard[];
  borderColor: string;
}) {
  return (
    <section className="mb-16">
      <div
        className={`flex items-center gap-4 mb-8 pl-4 border-l-4`}
        style={{ borderColor }}
      >
        <div
          className="w-14 h-14 rounded-xl flex items-center justify-center text-white font-bold text-lg shadow-md"
          style={{ backgroundColor: borderColor }}
        >
          S{weekNumber}
        </div>
        <div>
          <h3 className="text-xl font-bold text-[#1b3a4b]">{title}</h3>
          <p className="text-sm text-gray-500">{subtitle}</p>
        </div>
      </div>
      <div>
        {days.map((day) => (
          <DayCardComponent key={day.day} card={day} />
        ))}
      </div>
    </section>
  );
}

export default function ProgrammePage() {
  return (
    <main className="min-h-screen bg-gray-50">
      {/* ========== HERO ========== */}
      <section
        className="relative overflow-hidden text-white"
        style={{
          background: "linear-gradient(135deg, #1b3a4b 0%, #2d5f7a 100%)",
        }}
      >
        <div className="absolute inset-0 opacity-10">
          <div className="absolute top-10 left-10 w-72 h-72 bg-white rounded-full blur-3xl" />
          <div className="absolute bottom-10 right-10 w-96 h-96 bg-[#ff3621] rounded-full blur-3xl" />
        </div>

        <div className="relative max-w-5xl mx-auto px-6 py-20 text-center">
          <div className="inline-block bg-white/10 backdrop-blur border border-white/20 text-sm font-semibold px-5 py-2 rounded-full mb-8">
            🎯 Objectif : Niveau Data Engineer confirmé
          </div>
          <h1 className="text-4xl md:text-5xl font-extrabold leading-tight mb-6">
            Programme 15 Jours
            <br />
            pour Maîtriser{" "}
            <span className="text-amber-400">Databricks</span>
          </h1>
          <p className="text-lg md:text-xl text-white/80 max-w-2xl mx-auto mb-10">
            De zéro à Data Engineer prêt pour les grandes entreprises
          </p>

          <div className="flex flex-col sm:flex-row gap-4 justify-center">
            <Link
              href="/modules/1-0-creation-clusters"
              className="inline-flex items-center justify-center gap-2 bg-[#ff3621] hover:bg-[#e02e1a] text-white font-bold px-8 py-3.5 rounded-xl transition-colors shadow-lg shadow-[#ff3621]/25"
            >
              🚀 Commencer le programme
            </Link>
            <a
              href="#programme"
              className="inline-flex items-center justify-center gap-2 bg-white/10 hover:bg-white/20 text-white font-bold px-8 py-3.5 rounded-xl transition-colors border border-white/20"
            >
              📋 Voir le programme
            </a>
          </div>
        </div>
      </section>

      {/* ========== OBJECTIFS ========== */}
      <section className="max-w-6xl mx-auto px-6 py-16">
        <h2 className="text-2xl font-bold text-center text-[#1b3a4b] mb-3">
          🎯 Objectifs du programme
        </h2>
        <p className="text-center text-gray-500 mb-10 max-w-xl mx-auto">
          Ce que vous saurez faire à la fin des 15 jours
        </p>

        <div className="grid sm:grid-cols-2 lg:grid-cols-4 gap-5">
          {[
            {
              icon: "🏗️",
              text: "Maîtriser la plateforme Databricks et l'architecture Lakehouse",
            },
            {
              icon: "⚙️",
              text: "Construire des pipelines ETL/ELT de production",
            },
            {
              icon: "🔒",
              text: "Gérer la gouvernance et la sécurité des données",
            },
            {
              icon: "🎓",
              text: "Être prêt pour la certification et les entretiens techniques",
            },
          ].map((obj, i) => (
            <div
              key={i}
              className="bg-white rounded-xl border border-gray-200 p-6 shadow-sm hover:shadow-md hover:border-[#1b3a4b]/30 transition-all"
            >
              <div className="text-3xl mb-4">{obj.icon}</div>
              <p className="text-sm font-medium text-gray-700 leading-relaxed">
                {obj.text}
              </p>
            </div>
          ))}
        </div>
      </section>

      {/* ========== PROGRAMME JOUR PAR JOUR ========== */}
      <section id="programme" className="max-w-4xl mx-auto px-6 py-8">
        <h2 className="text-2xl font-bold text-center text-[#1b3a4b] mb-3">
          📅 Programme jour par jour
        </h2>
        <p className="text-center text-gray-500 mb-14 max-w-xl mx-auto">
          15 jours structurés pour une montée en compétences progressive
        </p>

        <WeekSection
          weekNumber={1}
          title="SEMAINE 1 : FONDATIONS"
          subtitle="Jours 1-5 • Maîtrisez les bases de Databricks"
          days={week1Days}
          borderColor="#1b3a4b"
        />

        <WeekSection
          weekNumber={2}
          title="SEMAINE 2 : PRODUCTION"
          subtitle="Jours 6-10 • Construisez des pipelines de production"
          days={week2Days}
          borderColor="#ff3621"
        />

        <WeekSection
          weekNumber={3}
          title="SEMAINE 3 : MAÎTRISE"
          subtitle="Jours 11-15 • Projets avancés et certification"
          days={week3Days}
          borderColor="#d97706"
        />
      </section>

      {/* ========== PRÉREQUIS ========== */}
      <section className="bg-white border-y border-gray-200">
        <div className="max-w-5xl mx-auto px-6 py-16">
          <h2 className="text-2xl font-bold text-center text-[#1b3a4b] mb-3">
            📋 Prérequis
          </h2>
          <p className="text-center text-gray-500 mb-10">
            Ce dont vous avez besoin avant de commencer
          </p>

          <div className="grid sm:grid-cols-2 lg:grid-cols-4 gap-5">
            {[
              {
                icon: "🗃️",
                title: "SQL de base",
                desc: "SELECT, WHERE, JOIN, GROUP BY",
              },
              {
                icon: "🐍",
                title: "Python basique",
                desc: "Variables, fonctions, boucles",
              },
              {
                icon: "📊",
                title: "Data Engineering",
                desc: "Notions de base (optionnel)",
              },
              {
                icon: "☁️",
                title: "Compte Databricks",
                desc: "Community Edition (gratuit)",
              },
            ].map((prereq, i) => (
              <div
                key={i}
                className="bg-gray-50 rounded-xl border border-gray-200 p-5 text-center"
              >
                <div className="text-3xl mb-3">{prereq.icon}</div>
                <h4 className="font-bold text-[#1b3a4b] mb-1">
                  {prereq.title}
                </h4>
                <p className="text-xs text-gray-500">{prereq.desc}</p>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* ========== CONSEILS ========== */}
      <section className="max-w-4xl mx-auto px-6 py-16">
        <h2 className="text-2xl font-bold text-center text-[#1b3a4b] mb-3">
          💡 Conseils pour réussir
        </h2>
        <p className="text-center text-gray-500 mb-10">
          Suivez ces recommandations pour tirer le maximum du programme
        </p>

        <div className="grid sm:grid-cols-2 gap-4">
          {[
            {
              icon: "🏋️",
              tip: "Pratiquez chaque exercice, ne vous contentez pas de lire",
              color: "border-l-[#ff3621]",
            },
            {
              icon: "💻",
              tip: "Utilisez Databricks Community Edition pour vous exercer gratuitement",
              color: "border-l-[#1b3a4b]",
            },
            {
              icon: "📝",
              tip: "Prenez des notes et créez vos propres résumés",
              color: "border-l-amber-500",
            },
            {
              icon: "👥",
              tip: "Rejoignez la communauté Databricks pour poser vos questions",
              color: "border-l-green-500",
            },
          ].map((conseil, i) => (
            <div
              key={i}
              className={`flex items-start gap-4 bg-white rounded-xl border border-gray-200 ${conseil.color} border-l-4 p-5 shadow-sm`}
            >
              <span className="text-2xl">{conseil.icon}</span>
              <p className="text-sm font-medium text-gray-700 leading-relaxed">
                {conseil.tip}
              </p>
            </div>
          ))}
        </div>
      </section>

      {/* ========== CTA BOTTOM ========== */}
      <section
        className="text-white text-center"
        style={{
          background: "linear-gradient(135deg, #1b3a4b 0%, #2d5f7a 100%)",
        }}
      >
        <div className="max-w-3xl mx-auto px-6 py-16">
          <h2 className="text-3xl font-extrabold mb-4">
            Prêt à devenir Data Engineer ?
          </h2>
          <p className="text-white/70 mb-8 max-w-lg mx-auto">
            Commencez dès maintenant votre parcours de 15 jours et maîtrisez
            Databricks de A à Z.
          </p>
          <div className="flex flex-col sm:flex-row gap-4 justify-center">
            <Link
              href="/modules/1-0-creation-clusters"
              className="inline-flex items-center justify-center gap-2 bg-[#ff3621] hover:bg-[#e02e1a] text-white font-bold px-8 py-4 rounded-xl transition-colors shadow-lg shadow-[#ff3621]/25 text-lg"
            >
              🚀 Commencer le Jour 1
            </Link>
            <Link
              href="/exercices"
              className="inline-flex items-center justify-center gap-2 bg-white/10 hover:bg-white/20 text-white font-bold px-8 py-4 rounded-xl transition-colors border border-white/20 text-lg"
            >
              📂 Voir les exercices
            </Link>
          </div>
        </div>
      </section>
    </main>
  );
}
