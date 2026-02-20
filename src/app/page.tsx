import Link from "next/link";
import HomeProgressSection from "@/components/HomeProgressSection";

const modules = [
  {
    number: 1,
    title: "Databricks Lakehouse Platform",
    description:
      "Découvrez la plateforme Databricks Lakehouse, apprenez à créer des clusters et maîtrisez les notebooks.",
    lessons: 2,
    href: "/modules/1-0-creation-clusters",
    icon: "🏗️",
    color: "from-blue-500 to-blue-700",
  },
  {
    number: 2,
    title: "ELT avec Spark SQL et Python",
    description:
      "Apprenez à manipuler les données avec Spark SQL et Python : bases de données, tables, vues et transformations.",
    lessons: 4,
    href: "/modules/2-1-bases-donnees-tables",
    icon: "⚡",
    color: "from-emerald-500 to-emerald-700",
  },
  {
    number: 3,
    title: "Traitement Incrémental des Données",
    description:
      "Maîtrisez le traitement de données en streaming avec Structured Streaming, Auto Loader et l'architecture Multi-Hop.",
    lessons: 3,
    href: "/modules/3-1-structured-streaming",
    icon: "🔄",
    color: "from-purple-500 to-purple-700",
  },
  {
    number: 4,
    title: "Pipelines de Production",
    description:
      "Construisez des pipelines de production robustes avec Delta Live Tables et l'orchestration de jobs Databricks.",
    lessons: 3,
    href: "/modules/4-1-delta-live-tables",
    icon: "🚀",
    color: "from-orange-500 to-orange-700",
  },
  {
    number: 5,
    title: "Gouvernance des Données",
    description:
      "Comprenez Unity Catalog et la gestion des permissions pour une gouvernance des données efficace.",
    lessons: 2,
    href: "/modules/5-1-unity-catalog",
    icon: "🛡️",
    color: "from-red-500 to-red-700",
  },
];

const concepts = [
  {
    title: "Delta Lake",
    description:
      "Format de stockage open-source offrant des transactions ACID, le time travel et la gestion de schéma sur un data lake.",
    icon: (
      <svg className="w-8 h-8" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
        <path strokeLinecap="round" strokeLinejoin="round" d="M20.25 6.375c0 2.278-3.694 4.125-8.25 4.125S3.75 8.653 3.75 6.375m16.5 0c0-2.278-3.694-4.125-8.25-4.125S3.75 4.097 3.75 6.375m16.5 0v11.25c0 2.278-3.694 4.125-8.25 4.125s-8.25-1.847-8.25-4.125V6.375m16.5 0v3.75m-16.5-3.75v3.75m16.5 0v3.75C20.25 16.153 16.556 18 12 18s-8.25-1.847-8.25-4.125v-3.75m16.5 0c0 2.278-3.694 4.125-8.25 4.125s-8.25-1.847-8.25-4.125" />
      </svg>
    ),
  },
  {
    title: "Lakehouse",
    description:
      "Architecture combinant les avantages d'un data warehouse (fiabilité, gouvernance) et d'un data lake (flexibilité, coût).",
    icon: (
      <svg className="w-8 h-8" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
        <path strokeLinecap="round" strokeLinejoin="round" d="M2.25 21h19.5m-18-18v18m10.5-18v18m6-13.5V21M6.75 6.75h.75m-.75 3h.75m-.75 3h.75m3-6h.75m-.75 3h.75m-.75 3h.75M6.75 21v-3.375c0-.621.504-1.125 1.125-1.125h2.25c.621 0 1.125.504 1.125 1.125V21M3 3h12m-.75 4.5H21m-3.75 3H21m-3.75 3H21" />
      </svg>
    ),
  },
  {
    title: "Spark SQL",
    description:
      "Moteur de traitement distribué permettant d'exécuter des requêtes SQL sur de grands volumes de données.",
    icon: (
      <svg className="w-8 h-8" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
        <path strokeLinecap="round" strokeLinejoin="round" d="M17.25 6.75 22.5 12l-5.25 5.25m-10.5 0L1.5 12l5.25-5.25m7.5-3-4.5 16.5" />
      </svg>
    ),
  },
  {
    title: "Structured Streaming",
    description:
      "API de Spark pour le traitement de flux de données en temps réel avec une sémantique exactly-once.",
    icon: (
      <svg className="w-8 h-8" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
        <path strokeLinecap="round" strokeLinejoin="round" d="M3.75 13.5l10.5-11.25L12 10.5h8.25L9.75 21.75 12 13.5H3.75z" />
      </svg>
    ),
  },
  {
    title: "Delta Live Tables",
    description:
      "Framework déclaratif pour construire des pipelines ETL fiables avec gestion automatique de la qualité des données.",
    icon: (
      <svg className="w-8 h-8" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
        <path strokeLinecap="round" strokeLinejoin="round" d="M3.375 19.5h17.25m-17.25 0a1.125 1.125 0 0 1-1.125-1.125M3.375 19.5h7.5c.621 0 1.125-.504 1.125-1.125m-9.75 0V5.625m0 12.75v-1.5c0-.621.504-1.125 1.125-1.125m18.375 2.625V5.625m0 12.75c0 .621-.504 1.125-1.125 1.125m1.125-1.125v-1.5c0-.621-.504-1.125-1.125-1.125m0 3.75h-7.5A1.125 1.125 0 0 1 12 18.375m9.75-12.75c0-.621-.504-1.125-1.125-1.125H3.375c-.621 0-1.125.504-1.125 1.125m19.5 0v1.5c0 .621-.504 1.125-1.125 1.125M2.25 5.625v1.5c0 .621.504 1.125 1.125 1.125m0 0h17.25m-17.25 0h7.5c.621 0 1.125.504 1.125 1.125M3.375 8.25c-.621 0-1.125.504-1.125 1.125v1.5c0 .621.504 1.125 1.125 1.125m17.25-3.75h-7.5c-.621 0-1.125.504-1.125 1.125m8.625-1.125c.621 0 1.125.504 1.125 1.125v1.5c0 .621-.504 1.125-1.125 1.125m-17.25 0h7.5m-7.5 0c-.621 0-1.125.504-1.125 1.125v1.5c0 .621.504 1.125 1.125 1.125M12 10.875v-1.5m0 1.5c0 .621-.504 1.125-1.125 1.125M12 10.875c0 .621.504 1.125 1.125 1.125m-2.25 0c.621 0 1.125.504 1.125 1.125M13.125 12h7.5m-7.5 0c-.621 0-1.125.504-1.125 1.125M20.625 12c.621 0 1.125.504 1.125 1.125v1.5c0 .621-.504 1.125-1.125 1.125m-17.25 0h7.5M12 14.625v-1.5m0 1.5c0 .621-.504 1.125-1.125 1.125M12 14.625c0 .621.504 1.125 1.125 1.125m-2.25 0c.621 0 1.125.504 1.125 1.125m0 0v1.5c0 .621-.504 1.125-1.125 1.125M3.375 15.75h7.5" />
      </svg>
    ),
  },
  {
    title: "Unity Catalog",
    description:
      "Solution de gouvernance unifiée pour gérer les permissions, le lignage et l'audit des données.",
    icon: (
      <svg className="w-8 h-8" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
        <path strokeLinecap="round" strokeLinejoin="round" d="M9 12.75 11.25 15 15 9.75m-3-7.036A11.959 11.959 0 0 1 3.598 6 11.99 11.99 0 0 0 3 9.749c0 5.592 3.824 10.29 9 11.623 5.176-1.332 9-6.03 9-11.622 0-1.31-.21-2.571-.598-3.751h-.152c-3.196 0-6.1-1.248-8.25-3.285Z" />
      </svg>
    ),
  },
];

export default function Home() {
  return (
    <main className="min-h-screen">
      {/* ── HERO SECTION ── */}
      <section
        className="relative overflow-hidden py-24 px-6 text-white"
        style={{
          background: "linear-gradient(135deg, #1b3a4b 0%, #2d5f7a 100%)",
        }}
      >
        {/* Decorative blurred circles */}
        <div className="absolute -top-24 -left-24 w-96 h-96 bg-white/5 rounded-full blur-3xl" />
        <div className="absolute -bottom-32 -right-32 w-120 h-120 bg-white/5 rounded-full blur-3xl" />

        <div className="relative max-w-5xl mx-auto text-center">
          <span className="inline-block mb-6 rounded-full border border-white/20 bg-white/10 px-5 py-1.5 text-sm font-medium tracking-wide backdrop-blur">
            5 Modules • 14 Leçons • Exercices • Programme 15 Jours
          </span>

          <h1 className="text-5xl sm:text-6xl font-extrabold leading-tight tracking-tight">
            Maîtrisez{" "}
            <span className="bg-linear-to-r from-amber-300 to-orange-400 bg-clip-text text-transparent">
              Databricks
            </span>
          </h1>
          <p className="mt-2 text-2xl sm:text-3xl font-semibold text-blue-200">
            Data Engineer Associate
          </p>

          <p className="mt-6 max-w-2xl mx-auto text-lg text-blue-100/90 leading-relaxed">
            Guide complet pour préparer la certification Databricks Certified
            Data Engineer Associate. Cours structurés, exercices pratiques,
            projets réels et préparation aux entretiens pour décrocher un poste
            dans les grandes entreprises.
          </p>

          <div className="mt-10 flex flex-col sm:flex-row items-center justify-center gap-4">
            <Link
              href="/modules/1-0-creation-clusters"
              className="inline-flex items-center gap-2 rounded-xl bg-amber-400 px-8 py-3.5 text-lg font-bold text-gray-900 shadow-lg shadow-amber-400/25 transition hover:bg-amber-300 hover:shadow-amber-300/30 hover:scale-105"
            >
              Commencer le cours
              <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2.5}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M13.5 4.5 21 12m0 0-7.5 7.5M21 12H3" />
              </svg>
            </Link>
            <Link
              href="/programme"
              className="inline-flex items-center gap-2 rounded-xl border-2 border-white/30 bg-white/10 backdrop-blur px-8 py-3.5 text-lg font-bold text-white transition hover:bg-white/20 hover:scale-105"
            >
              Programme 15 jours
              <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2.5}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M6.75 3v2.25M17.25 3v2.25M3 18.75V7.5a2.25 2.25 0 012.25-2.25h13.5A2.25 2.25 0 0121 7.5v11.25m-18 0A2.25 2.25 0 005.25 21h13.5A2.25 2.25 0 0021 18.75m-18 0v-7.5A2.25 2.25 0 015.25 9h13.5A2.25 2.25 0 0121 11.25v7.5" />
              </svg>
            </Link>
          </div>
        </div>
      </section>

      {/* ── PROGRESS DASHBOARD ── */}
      <HomeProgressSection />

      {/* ── MODULES OVERVIEW ── */}
      <section className="py-20 px-6 bg-gray-50">
        <div className="max-w-6xl mx-auto">
          <h2 className="text-3xl sm:text-4xl font-bold text-center text-gray-900">
            Parcours de Formation
          </h2>
          <p className="mt-3 text-center text-gray-500 max-w-xl mx-auto">
            Progressez étape par étape à travers 5&nbsp;modules structurés pour
            maîtriser Databricks.
          </p>

          <div className="mt-14 grid gap-8 sm:grid-cols-2 lg:grid-cols-3">
            {modules.map((m) => (
              <Link
                key={m.number}
                href={m.href}
                className="group relative flex flex-col rounded-2xl bg-white border border-gray-200 p-6 shadow-sm transition hover:shadow-xl hover:-translate-y-1"
              >
                {/* Module number badge */}
                <span
                  className={`inline-flex w-12 h-12 items-center justify-center rounded-xl bg-linear-to-br ${m.color} text-white text-lg font-bold shadow-md`}
                >
                  {m.number}
                </span>

                <h3 className="mt-4 text-xl font-semibold text-gray-900 group-hover:text-blue-700 transition-colors">
                  <span className="mr-2">{m.icon}</span>
                  {m.title}
                </h3>

                <p className="mt-2 text-gray-500 text-sm leading-relaxed flex-1">
                  {m.description}
                </p>

                <div className="mt-5 flex items-center justify-between text-sm">
                  <span className="text-gray-400 font-medium">
                    {m.lessons} leçon{m.lessons > 1 ? "s" : ""}
                  </span>
                  <span className="inline-flex items-center gap-1 font-semibold text-blue-600 group-hover:gap-2 transition-all">
                    Explorer
                    <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                      <path strokeLinecap="round" strokeLinejoin="round" d="M13.5 4.5 21 12m0 0-7.5 7.5M21 12H3" />
                    </svg>
                  </span>
                </div>
              </Link>
            ))}
          </div>
        </div>
      </section>

      {/* ── KEY CONCEPTS ── */}
      <section className="py-20 px-6 bg-white">
        <div className="max-w-6xl mx-auto">
          <h2 className="text-3xl sm:text-4xl font-bold text-center text-gray-900">
            Concepts Clés de la Certification
          </h2>
          <p className="mt-3 text-center text-gray-500 max-w-xl mx-auto">
            Les technologies et paradigmes fondamentaux à connaître pour
            réussir l&apos;examen.
          </p>

          <div className="mt-14 grid gap-6 sm:grid-cols-2 lg:grid-cols-3">
            {concepts.map((c) => (
              <div
                key={c.title}
                className="rounded-2xl border border-gray-100 bg-linear-to-b from-gray-50 to-white p-6 shadow-sm hover:shadow-md transition"
              >
                <div className="inline-flex items-center justify-center w-14 h-14 rounded-xl bg-blue-50 text-blue-600">
                  {c.icon}
                </div>
                <h3 className="mt-4 text-lg font-semibold text-gray-900">
                  {c.title}
                </h3>
                <p className="mt-2 text-sm text-gray-500 leading-relaxed">
                  {c.description}
                </p>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* ── PROGRAMME 15 JOURS ── */}
      <section className="py-20 px-6 bg-gray-50">
        <div className="max-w-6xl mx-auto">
          <h2 className="text-3xl sm:text-4xl font-bold text-center text-gray-900">
            🗓️ Programme en 15 Jours
          </h2>
          <p className="mt-3 text-center text-gray-500 max-w-xl mx-auto">
            Un plan structuré pour passer de zéro à un niveau professionnel, prêt pour les grandes entreprises.
          </p>

          <div className="mt-14 grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
            {[
              { days: "J1-J3", title: "Fondamentaux", desc: "Plateforme Databricks, clusters, notebooks, SQL de base", color: "from-blue-500 to-blue-600", emoji: "📘" },
              { days: "J4-J5", title: "Tables & Transformations", desc: "Tables managed/external, vues, CTEs, MERGE INTO, UDFs", color: "from-emerald-500 to-emerald-600", emoji: "⚡" },
              { days: "J6-J7", title: "Streaming", desc: "Structured Streaming, Auto Loader, architecture Medallion", color: "from-purple-500 to-purple-600", emoji: "🌊" },
              { days: "J8-J9", title: "Production", desc: "Delta Live Tables, orchestration Jobs, monitoring", color: "from-orange-500 to-orange-600", emoji: "🚀" },
              { days: "J10-J11", title: "Gouvernance & Projets", desc: "Unity Catalog, permissions, projet e-commerce", color: "from-red-500 to-red-600", emoji: "🛡️" },
              { days: "J12-J15", title: "Projet Final & Entretiens", desc: "Cas SNCF complet, quiz certification, préparation entretiens", color: "from-amber-500 to-amber-600", emoji: "🚄" },
            ].map((phase) => (
              <div key={phase.days} className="rounded-xl bg-white border border-gray-200 p-5 shadow-sm hover:shadow-md transition">
                <div className="flex items-center gap-3 mb-3">
                  <span className={`inline-flex items-center justify-center w-10 h-10 rounded-lg bg-linear-to-br ${phase.color} text-white text-sm font-bold shadow`}>
                    {phase.days}
                  </span>
                  <span className="text-xl">{phase.emoji}</span>
                </div>
                <h3 className="text-lg font-semibold text-gray-900">{phase.title}</h3>
                <p className="mt-1 text-sm text-gray-500">{phase.desc}</p>
              </div>
            ))}
          </div>

          <div className="mt-10 text-center">
            <Link
              href="/programme"
              className="inline-flex items-center gap-2 rounded-xl bg-[#1b3a4b] px-8 py-3.5 text-lg font-bold text-white shadow-lg transition hover:bg-[#2d5f7a] hover:scale-105"
            >
              Voir le programme détaillé
              <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2.5}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M13.5 4.5 21 12m0 0-7.5 7.5M21 12H3" />
              </svg>
            </Link>
          </div>
        </div>
      </section>

      {/* ── EXERCICES & PROJETS ── */}
      <section className="py-20 px-6 bg-white">
        <div className="max-w-6xl mx-auto">
          <h2 className="text-3xl sm:text-4xl font-bold text-center text-gray-900">
            💪 Exercices & Projets Pratiques
          </h2>
          <p className="mt-3 text-center text-gray-500 max-w-xl mx-auto">
            Mettez en pratique vos connaissances avec des exercices progressifs et des projets réalistes.
          </p>

          <div className="mt-14 grid gap-6 sm:grid-cols-2 lg:grid-cols-4">
            {[
              { title: "Exercices Guidés", count: "5 séries", desc: "Fondamentaux → Delta Lake avancé", href: "/exercices", emoji: "📝", badge: "bg-green-100 text-green-800" },
              { title: "Projet E-commerce", count: "6h", desc: "Pipeline complet Bronze→Silver→Gold", href: "/exercices/projet-ecommerce", emoji: "🛒", badge: "bg-amber-100 text-amber-800" },
              { title: "Projet SNCF", count: "14h", desc: "Cas réel : trafic, retards, maintenance", href: "/exercices/projet-final", emoji: "🚄", badge: "bg-red-100 text-red-800" },
              { title: "Prépa Entretiens", count: "20 questions", desc: "Prêt pour SNCF, BNP, Total, Orange...", href: "/exercices/preparation-entretiens", emoji: "🎯", badge: "bg-purple-100 text-purple-800" },
            ].map((item) => (
              <Link
                key={item.title}
                href={item.href}
                className="group flex flex-col rounded-2xl border border-gray-200 bg-linear-to-b from-gray-50 to-white p-6 shadow-sm hover:shadow-lg hover:-translate-y-1 transition-all"
              >
                <span className="text-3xl mb-3">{item.emoji}</span>
                <h3 className="text-lg font-semibold text-gray-900 group-hover:text-[#ff3621] transition-colors">
                  {item.title}
                </h3>
                <span className={`mt-2 inline-block w-fit text-xs font-semibold px-2.5 py-0.5 rounded-full ${item.badge}`}>
                  {item.count}
                </span>
                <p className="mt-2 text-sm text-gray-500 flex-1">{item.desc}</p>
              </Link>
            ))}
          </div>

          <div className="mt-10 text-center">
            <Link
              href="/exercices"
              className="inline-flex items-center gap-2 rounded-xl bg-[#ff3621] px-8 py-3.5 text-lg font-bold text-white shadow-lg transition hover:bg-[#e02e1a] hover:scale-105"
            >
              Tous les exercices
              <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2.5}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M13.5 4.5 21 12m0 0-7.5 7.5M21 12H3" />
              </svg>
            </Link>
          </div>
        </div>
      </section>

      {/* ── ARCHITECTURE DIAGRAM ── */}
      <section className="py-20 px-6 bg-gray-50">
        <div className="max-w-4xl mx-auto">
          <h2 className="text-3xl sm:text-4xl font-bold text-center text-gray-900">
            Architecture Lakehouse
          </h2>
          <p className="mt-3 text-center text-gray-500 max-w-xl mx-auto">
            Vue d&apos;ensemble de l&apos;architecture médaillon (Bronze → Silver → Gold)
            utilisée dans Databricks.
          </p>

          {/* Diagram */}
          <div className="mt-14 flex flex-col items-center gap-4">
            {/* Top: BI & ML */}
            <div className="w-full max-w-2xl rounded-xl bg-linear-to-r from-violet-600 to-purple-700 text-white text-center py-4 px-6 shadow-lg">
              <p className="text-xs uppercase tracking-widest text-violet-200 font-semibold">
                Consommation
              </p>
              <p className="mt-1 text-lg font-bold">
                Business Intelligence &amp; Machine Learning
              </p>
            </div>

            {/* Arrow */}
            <svg className="w-6 h-8 text-gray-400" fill="currentColor" viewBox="0 0 24 32">
              <path d="M12 0v24m0 0-6-6m6 6 6-6" stroke="currentColor" strokeWidth="2" fill="none" />
            </svg>

            {/* Medallion layers */}
            <div className="w-full max-w-2xl grid grid-cols-3 gap-3">
              <div className="rounded-xl bg-linear-to-b from-yellow-400 to-yellow-500 text-center py-5 px-3 shadow-md">
                <p className="text-xs uppercase tracking-widest font-semibold text-yellow-900/70">
                  Couche
                </p>
                <p className="mt-1 text-xl font-extrabold text-yellow-900">
                  🥇 Gold
                </p>
                <p className="mt-1 text-xs text-yellow-800">
                  Agrégats métier
                </p>
              </div>
              <div className="rounded-xl bg-linear-to-b from-gray-300 to-gray-400 text-center py-5 px-3 shadow-md">
                <p className="text-xs uppercase tracking-widest font-semibold text-gray-700/70">
                  Couche
                </p>
                <p className="mt-1 text-xl font-extrabold text-gray-800">
                  🥈 Silver
                </p>
                <p className="mt-1 text-xs text-gray-700">
                  Données nettoyées
                </p>
              </div>
              <div className="rounded-xl bg-linear-to-b from-amber-600 to-amber-700 text-center py-5 px-3 shadow-md">
                <p className="text-xs uppercase tracking-widest font-semibold text-amber-200/80">
                  Couche
                </p>
                <p className="mt-1 text-xl font-extrabold text-white">
                  🥉 Bronze
                </p>
                <p className="mt-1 text-xs text-amber-100">
                  Données brutes
                </p>
              </div>
            </div>

            {/* Arrow */}
            <svg className="w-6 h-8 text-gray-400" fill="currentColor" viewBox="0 0 24 32">
              <path d="M12 0v24m0 0-6-6m6 6 6-6" stroke="currentColor" strokeWidth="2" fill="none" />
            </svg>

            {/* Delta Lake */}
            <div className="w-full max-w-2xl rounded-xl bg-linear-to-r from-sky-600 to-cyan-600 text-white text-center py-4 px-6 shadow-lg">
              <p className="text-xs uppercase tracking-widest text-sky-200 font-semibold">
                Format de Stockage
              </p>
              <p className="mt-1 text-lg font-bold">Delta Lake</p>
              <p className="text-xs text-sky-100 mt-0.5">
                Transactions ACID • Time Travel • Schema Evolution
              </p>
            </div>

            {/* Arrow */}
            <svg className="w-6 h-8 text-gray-400" fill="currentColor" viewBox="0 0 24 32">
              <path d="M12 0v24m0 0-6-6m6 6 6-6" stroke="currentColor" strokeWidth="2" fill="none" />
            </svg>

            {/* Cloud Storage */}
            <div className="w-full max-w-2xl rounded-xl bg-linear-to-r from-gray-700 to-gray-900 text-white text-center py-4 px-6 shadow-lg">
              <p className="text-xs uppercase tracking-widest text-gray-400 font-semibold">
                Infrastructure
              </p>
              <p className="mt-1 text-lg font-bold">
                ☁️ Cloud Object Storage
              </p>
              <p className="text-xs text-gray-300 mt-0.5">
                AWS S3 • Azure ADLS • Google GCS
              </p>
            </div>
          </div>
        </div>
      </section>
    </main>
  );
}
