"use client";

import { useState } from "react";
import Link from "next/link";
import CodeBlock from "@/components/CodeBlock";
import InfoBox from "@/components/InfoBox";

function SolutionToggle({
  id,
  children,
}: {
  id: string;
  children: React.ReactNode;
}) {
  const [open, setOpen] = useState(false);

  return (
    <div className="mt-4">
      <button
        onClick={() => setOpen(!open)}
        className="inline-flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-semibold bg-[#1b3a4b] text-white hover:bg-[#2d5f7a] transition-colors"
        aria-expanded={open}
        aria-controls={id}
      >
        {open ? "🙈 Masquer la solution" : "👁️ Voir la solution"}
      </button>
      {open && (
        <div
          id={id}
          className="mt-4 border-l-4 border-[#ff3621] pl-5 space-y-4"
        >
          {children}
        </div>
      )}
    </div>
  );
}

export default function ProjetIoTPage() {
  return (
    <div className="min-h-[calc(100vh-4rem)]">
      {/* Hero */}
      <div className="relative bg-gradient-to-br from-[#1b3a4b] via-[#2d5f7a] to-[#1b3a4b] text-white overflow-hidden">
        <div className="absolute inset-0 opacity-10">
          <div className="absolute top-10 left-10 w-72 h-72 bg-[#ff3621] rounded-full blur-3xl" />
          <div className="absolute bottom-10 right-10 w-96 h-96 bg-blue-400 rounded-full blur-3xl" />
        </div>
        <div className="relative max-w-4xl mx-auto px-6 py-14 lg:py-18">
          <div className="flex items-center gap-3 mb-4">
            <span className="inline-flex items-center px-3 py-1 rounded-full text-xs font-semibold bg-red-400/20 text-red-200 border border-red-400/30">
              Avancé
            </span>
            <span className="text-sm text-white/70">⏱ 6 heures</span>
            <span className="text-sm text-white/70">📅 Jour 10</span>
          </div>
          <h1 className="text-3xl lg:text-4xl font-extrabold mb-3">
            📡 Mini-Projet : Pipeline IoT Streaming
          </h1>
          <p className="text-lg text-white/80 max-w-2xl leading-relaxed">
            Créez un pipeline streaming temps réel pour surveiller des
            capteurs IoT industriels — détection d&apos;anomalies, alertes
            et dashboards agrégés.
          </p>
        </div>
      </div>

      {/* Content */}
      <div className="max-w-4xl mx-auto px-6 py-10 lg:px-10">
        {/* Navigation */}
        <div className="flex flex-wrap gap-3 mb-10">
          <Link
            href="/exercices"
            className="inline-flex items-center gap-2 text-sm text-[#1b3a4b] hover:text-[#ff3621] transition-colors"
          >
            ← Tous les exercices
          </Link>
          <span className="text-gray-300">|</span>
          <Link
            href="/programme"
            className="inline-flex items-center gap-2 text-sm text-[#1b3a4b] hover:text-[#ff3621] transition-colors"
          >
            📅 Programme complet
          </Link>
        </div>

        {/* Contexte du projet */}
        <section className="mb-10">
          <h2 className="text-2xl font-bold text-[#1b3a4b] mb-4">
            📖 Contexte du projet
          </h2>
          <p className="text-gray-700 leading-relaxed mb-4">
            Vous êtes <strong>Data Engineer</strong> dans une usine qui
            surveille des capteurs IoT (température, pression, humidité). Les
            capteurs envoient des données en continu. Vous devez créer un
            pipeline streaming pour <strong>détecter les anomalies</strong>{" "}
            et fournir des <strong>dashboards temps réel</strong>.
          </p>

          <div className="bg-gray-50 rounded-xl border border-gray-200 p-5 mb-6">
            <h3 className="text-lg font-bold text-[#1b3a4b] mb-3">
              🎯 Objectifs
            </h3>
            <ul className="space-y-2 text-sm text-gray-700">
              <li className="flex items-center gap-2">
                <span className="text-green-500">✓</span>
                Simuler des données capteurs avec anomalies intégrées
              </li>
              <li className="flex items-center gap-2">
                <span className="text-green-500">✓</span>
                Ingérer les données en streaming avec Auto Loader
              </li>
              <li className="flex items-center gap-2">
                <span className="text-green-500">✓</span>
                Détecter automatiquement les anomalies (température,
                pression, humidité)
              </li>
              <li className="flex items-center gap-2">
                <span className="text-green-500">✓</span>
                Créer des agrégations fenêtrées pour le monitoring
              </li>
              <li className="flex items-center gap-2">
                <span className="text-green-500">✓</span>
                Générer des alertes en temps réel
              </li>
            </ul>
          </div>
        </section>

        {/* Architecture */}
        <section className="mb-10">
          <h2 className="text-2xl font-bold text-[#1b3a4b] mb-4">
            🏗️ Architecture du projet
          </h2>
          <div className="bg-white rounded-xl border border-gray-200 p-6 overflow-x-auto">
            <div className="flex items-center justify-between gap-3 min-w-[700px]">
              {/* Sources IoT */}
              <div className="flex flex-col items-center gap-2">
                <div className="bg-indigo-100 border-2 border-indigo-300 rounded-xl px-4 py-3 text-center min-w-[120px]">
                  <div className="text-2xl mb-1">📡</div>
                  <div className="font-bold text-indigo-800 text-sm">
                    Capteurs IoT
                  </div>
                  <div className="text-xs text-indigo-600 mt-1">
                    Temp / Pression / Humidité
                  </div>
                </div>
              </div>

              <div className="text-2xl text-gray-400 font-bold">→</div>

              {/* Auto Loader */}
              <div className="flex flex-col items-center gap-2">
                <div className="bg-cyan-100 border-2 border-cyan-300 rounded-xl px-4 py-3 text-center min-w-[120px]">
                  <div className="text-2xl mb-1">⚡</div>
                  <div className="font-bold text-cyan-800 text-sm">
                    Auto Loader
                  </div>
                  <div className="text-xs text-cyan-600 mt-1">
                    Ingestion streaming
                  </div>
                </div>
              </div>

              <div className="text-2xl text-gray-400 font-bold">→</div>

              {/* Bronze */}
              <div className="flex flex-col items-center gap-2">
                <div className="bg-amber-100 border-2 border-amber-300 rounded-xl px-4 py-3 text-center min-w-[120px]">
                  <div className="text-2xl mb-1">🥉</div>
                  <div className="font-bold text-amber-800 text-sm">
                    Bronze
                  </div>
                  <div className="text-xs text-amber-600 mt-1">
                    Raw data
                  </div>
                </div>
              </div>

              <div className="text-2xl text-gray-400 font-bold">→</div>

              {/* Silver */}
              <div className="flex flex-col items-center gap-2">
                <div className="bg-slate-100 border-2 border-slate-300 rounded-xl px-4 py-3 text-center min-w-[120px]">
                  <div className="text-2xl mb-1">🥈</div>
                  <div className="font-bold text-slate-700 text-sm">
                    Silver
                  </div>
                  <div className="text-xs text-slate-500 mt-1">
                    Anomalies + Alertes
                  </div>
                </div>
              </div>

              <div className="text-2xl text-gray-400 font-bold">→</div>

              {/* Gold */}
              <div className="flex flex-col items-center gap-2">
                <div className="bg-yellow-100 border-2 border-yellow-300 rounded-xl px-4 py-3 text-center min-w-[120px]">
                  <div className="text-2xl mb-1">🥇</div>
                  <div className="font-bold text-yellow-800 text-sm">
                    Gold
                  </div>
                  <div className="text-xs text-yellow-600 mt-1">
                    Agrégations fenêtrées
                  </div>
                </div>
              </div>
            </div>
          </div>
        </section>

        {/* Sommaire */}
        <div className="bg-gray-50 rounded-xl border border-gray-200 p-5 mb-10">
          <h2 className="text-lg font-bold text-[#1b3a4b] mb-3">
            📋 Sommaire des étapes
          </h2>
          <ol className="space-y-2 text-sm text-gray-700">
            <li className="flex items-center gap-2">
              <span className="w-6 h-6 flex items-center justify-center bg-[#1b3a4b] text-white text-xs font-bold rounded-full">
                1
              </span>
              <span>
                Simulation des données capteurs{" "}
                <span className="text-gray-400">(30 min)</span>
              </span>
            </li>
            <li className="flex items-center gap-2">
              <span className="w-6 h-6 flex items-center justify-center bg-[#1b3a4b] text-white text-xs font-bold rounded-full">
                2
              </span>
              <span>
                Couche Bronze — Ingestion Auto Loader{" "}
                <span className="text-gray-400">(45 min)</span>
              </span>
            </li>
            <li className="flex items-center gap-2">
              <span className="w-6 h-6 flex items-center justify-center bg-[#1b3a4b] text-white text-xs font-bold rounded-full">
                3
              </span>
              <span>
                Couche Silver — Détection d&apos;anomalies{" "}
                <span className="text-gray-400">(1h)</span>
              </span>
            </li>
            <li className="flex items-center gap-2">
              <span className="w-6 h-6 flex items-center justify-center bg-[#1b3a4b] text-white text-xs font-bold rounded-full">
                4
              </span>
              <span>
                Couche Gold — Agrégations fenêtrées{" "}
                <span className="text-gray-400">(1h30)</span>
              </span>
            </li>
            <li className="flex items-center gap-2">
              <span className="w-6 h-6 flex items-center justify-center bg-[#1b3a4b] text-white text-xs font-bold rounded-full">
                5
              </span>
              <span>
                Dashboard &amp; Requêtes d&apos;analyse{" "}
                <span className="text-gray-400">(30 min)</span>
              </span>
            </li>
          </ol>
        </div>

        {/* ====================== ÉTAPE 1 ====================== */}
        <section className="mb-14">
          <div className="flex items-center gap-3 mb-2">
            <span className="w-8 h-8 flex items-center justify-center bg-[#ff3621] text-white text-sm font-bold rounded-full">
              1
            </span>
            <h2 className="text-2xl font-bold text-[#1b3a4b]">
              Simulation des données capteurs
            </h2>
          </div>
          <div className="flex items-center gap-3 mb-5 ml-11">
            <span className="text-xs font-medium bg-gray-100 text-gray-600 px-2.5 py-1 rounded-full">
              ⏱ 30 min
            </span>
          </div>

          <div className="ml-11 space-y-4">
            <h3 className="text-lg font-semibold text-[#1b3a4b]">
              📖 Objectif
            </h3>
            <p className="text-gray-700 leading-relaxed">
              Générer des données simulées de capteurs IoT avec des mesures
              de température, pression et humidité. Environ 10% des données
              seront des <strong>anomalies</strong> (valeurs hors limites
              normales).
            </p>

            <InfoBox type="info" title="Simulation réaliste">
              Les valeurs normales suivent une distribution gaussienne
              centrée sur des valeurs typiques (22°C, 1013 hPa, 55%
              d&apos;humidité). Les anomalies sont générées avec des centres
              décalés pour simuler des dysfonctionnements.
            </InfoBox>

            <h3 className="text-lg font-semibold text-[#1b3a4b]">
              📝 Instructions
            </h3>
            <ol className="list-decimal list-inside space-y-2 text-gray-700">
              <li>
                Créez la base de données <code>iot</code> si elle n&apos;existe
                pas.
              </li>
              <li>
                Définissez 5 capteurs dans 5 emplacements différents de
                l&apos;usine.
              </li>
              <li>
                Générez 5 lots de 50 enregistrements chacun (250 mesures au
                total).
              </li>
              <li>
                Intégrez environ 10% d&apos;anomalies dans les données.
              </li>
            </ol>

            <SolutionToggle id="sol-iot-etape1">
              <p className="text-sm font-semibold text-gray-600 mb-2">
                Code complet — Simulation des capteurs :
              </p>
              <CodeBlock
                language="python"
                title="Création de la base et simulation"
                code={`# Créer la base de données
spark.sql("CREATE DATABASE IF NOT EXISTS iot")
spark.sql("USE iot")`}
              />
              <CodeBlock
                language="python"
                title="Générateur de données capteurs"
                code={`import json, random
from datetime import datetime, timedelta

sensors = ["SENSOR_001", "SENSOR_002", "SENSOR_003", "SENSOR_004", "SENSOR_005"]
locations = ["Hall_A", "Hall_B", "Hall_C", "Exterieur", "Salle_Machines"]

def generate_batch(batch_id, n=50):
    data = []
    for i in range(n):
        sensor_id = random.choice(sensors)
        loc_idx = sensors.index(sensor_id)
        
        # Générer des données normales avec quelques anomalies
        is_anomaly = random.random() < 0.1  # 10% d'anomalies
        temp = random.gauss(22, 2) if not is_anomaly else random.gauss(45, 5)
        pressure = random.gauss(1013, 5) if not is_anomaly else random.gauss(1050, 10)
        humidity = random.gauss(55, 10) if not is_anomaly else random.gauss(95, 5)
        
        record = {
            "sensor_id": sensor_id,
            "location": locations[loc_idx],
            "temperature": round(temp, 2),
            "pressure": round(pressure, 2),
            "humidity": round(humidity, 2),
            "timestamp": (datetime.now() - timedelta(
                minutes=random.randint(0, 60)
            )).isoformat(),
            "battery_level": round(random.uniform(10, 100), 1)
        }
        data.append(record)
    
    dbutils.fs.put(
        f"/tmp/iot/raw/batch_{batch_id}.json",
        "\\n".join([json.dumps(d) for d in data]), True
    )
    return len(data)

# Générer 5 lots
for i in range(5):
    n = generate_batch(i)
    print(f"✅ Batch {i}: {n} enregistrements générés")`}
              />
            </SolutionToggle>

            <InfoBox type="tip" title="Astuce">
              Utilisez{" "}
              <code>display(spark.read.json(&quot;/tmp/iot/raw/&quot;))</code>{" "}
              pour visualiser un échantillon des données générées et vérifier
              la présence d&apos;anomalies.
            </InfoBox>
          </div>
        </section>

        {/* ====================== ÉTAPE 2 ====================== */}
        <section className="mb-14">
          <div className="flex items-center gap-3 mb-2">
            <span className="w-8 h-8 flex items-center justify-center bg-[#ff3621] text-white text-sm font-bold rounded-full">
              2
            </span>
            <h2 className="text-2xl font-bold text-[#1b3a4b]">
              Couche Bronze — Ingestion Auto Loader
            </h2>
          </div>
          <div className="flex items-center gap-3 mb-5 ml-11">
            <span className="text-xs font-medium bg-gray-100 text-gray-600 px-2.5 py-1 rounded-full">
              ⏱ 45 min
            </span>
          </div>

          <div className="ml-11 space-y-4">
            <h3 className="text-lg font-semibold text-[#1b3a4b]">
              📖 Objectif
            </h3>
            <p className="text-gray-700 leading-relaxed">
              Ingérer les données brutes des capteurs dans la couche Bronze
              avec <strong>Auto Loader</strong> (<code>cloudFiles</code>).
              Ajoutez les métadonnées d&apos;ingestion : fichier source et
              horodatage.
            </p>

            <InfoBox type="info" title="Auto Loader pour l'IoT">
              Auto Loader est idéal pour l&apos;IoT : il détecte
              automatiquement les nouveaux fichiers déposés et les ingère
              de manière incrémentale, sans retraiter les fichiers déjà
              lus.
            </InfoBox>

            <h3 className="text-lg font-semibold text-[#1b3a4b]">
              📝 Instructions
            </h3>
            <ol className="list-decimal list-inside space-y-2 text-gray-700">
              <li>
                Configurez un stream Auto Loader sur le répertoire{" "}
                <code>/tmp/iot/raw/</code>.
              </li>
              <li>
                Ajoutez les colonnes <code>_source_file</code> et{" "}
                <code>_ingestion_time</code>.
              </li>
              <li>
                Écrivez dans la table <code>iot.bronze_sensors</code> avec
                un checkpoint dédié.
              </li>
            </ol>

            <SolutionToggle id="sol-iot-etape2">
              <p className="text-sm font-semibold text-gray-600 mb-2">
                Code complet — Couche Bronze :
              </p>
              <CodeBlock
                language="python"
                title="Bronze — Ingestion Auto Loader"
                code={`from pyspark.sql.functions import current_timestamp, input_file_name

bronze_iot = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/tmp/schema/iot")
    .load("/tmp/iot/raw/")
    .withColumn("_source_file", input_file_name())
    .withColumn("_ingestion_time", current_timestamp())
)

bronze_iot.writeStream \\
    .option("checkpointLocation", "/tmp/checkpoint/iot_bronze") \\
    .trigger(availableNow=True) \\
    .table("iot.bronze_sensors") \\
    .awaitTermination()

print("✅ Bronze IoT ingérée")
display(spark.table("iot.bronze_sensors").limit(5))`}
              />
            </SolutionToggle>

            <InfoBox type="warning" title="Schema Evolution">
              L&apos;option <code>cloudFiles.schemaLocation</code> est
              essentielle : elle stocke le schéma inféré pour gérer
              l&apos;évolution du schéma si de nouveaux champs apparaissent
              dans les données capteurs.
            </InfoBox>
          </div>
        </section>

        {/* ====================== ÉTAPE 3 ====================== */}
        <section className="mb-14">
          <div className="flex items-center gap-3 mb-2">
            <span className="w-8 h-8 flex items-center justify-center bg-[#ff3621] text-white text-sm font-bold rounded-full">
              3
            </span>
            <h2 className="text-2xl font-bold text-[#1b3a4b]">
              Couche Silver — Détection d&apos;anomalies
            </h2>
          </div>
          <div className="flex items-center gap-3 mb-5 ml-11">
            <span className="text-xs font-medium bg-gray-100 text-gray-600 px-2.5 py-1 rounded-full">
              ⏱ 1h
            </span>
          </div>

          <div className="ml-11 space-y-4">
            <h3 className="text-lg font-semibold text-[#1b3a4b]">
              📖 Objectif
            </h3>
            <p className="text-gray-700 leading-relaxed">
              Appliquer des règles de détection d&apos;anomalies sur chaque
              mesure. Chaque enregistrement reçoit un{" "}
              <strong>score d&apos;anomalie</strong> et un niveau de{" "}
              <strong>sévérité</strong> (NORMAL, WARNING, CRITICAL). Les
              alertes sont extraites dans une table dédiée.
            </p>

            <InfoBox type="info" title="Seuils d'anomalie">
              <div className="space-y-1">
                <p>
                  <strong>Température :</strong> 15°C – 35°C (hors plage =
                  anomalie)
                </p>
                <p>
                  <strong>Pression :</strong> 990 – 1030 hPa (hors plage =
                  anomalie)
                </p>
                <p>
                  <strong>Humidité :</strong> &gt; 85% = anomalie
                </p>
                <p className="mt-2 font-semibold">
                  Score 0 = NORMAL | Score 1 = WARNING | Score ≥ 2 = CRITICAL
                </p>
              </div>
            </InfoBox>

            <h3 className="text-lg font-semibold text-[#1b3a4b]">
              📝 Instructions
            </h3>
            <ol className="list-decimal list-inside space-y-2 text-gray-700">
              <li>
                Lisez <code>iot.bronze_sensors</code> en streaming.
              </li>
              <li>
                Filtrez les enregistrements avec <code>sensor_id</code> ou{" "}
                <code>timestamp</code> null.
              </li>
              <li>
                Ajoutez des colonnes booléennes pour chaque type
                d&apos;anomalie.
              </li>
              <li>
                Calculez un <code>anomaly_score</code> et une{" "}
                <code>severity</code>.
              </li>
              <li>
                Créez une table d&apos;alertes ne contenant que les
                WARNING et CRITICAL.
              </li>
            </ol>

            <SolutionToggle id="sol-iot-etape3">
              <p className="text-sm font-semibold text-gray-600 mb-2">
                Code complet — Détection d&apos;anomalies :
              </p>
              <CodeBlock
                language="python"
                title="Silver — Scoring et sévérité"
                code={`from pyspark.sql.functions import col, when, to_timestamp, current_timestamp

# Définir les seuils d'anomalie
TEMP_MIN, TEMP_MAX = 15.0, 35.0
PRESSURE_MIN, PRESSURE_MAX = 990.0, 1030.0
HUMIDITY_MAX = 85.0

silver_iot = (spark.readStream
    .table("iot.bronze_sensors")
    .filter("sensor_id IS NOT NULL AND timestamp IS NOT NULL")
    .withColumn("event_time", to_timestamp("timestamp"))
    .withColumn("is_temp_anomaly", 
        when((col("temperature") < TEMP_MIN) | 
             (col("temperature") > TEMP_MAX), True)
        .otherwise(False))
    .withColumn("is_pressure_anomaly",
        when((col("pressure") < PRESSURE_MIN) | 
             (col("pressure") > PRESSURE_MAX), True)
        .otherwise(False))
    .withColumn("is_humidity_anomaly",
        when(col("humidity") > HUMIDITY_MAX, True)
        .otherwise(False))
    .withColumn("anomaly_score",
        col("is_temp_anomaly").cast("int") + 
        col("is_pressure_anomaly").cast("int") + 
        col("is_humidity_anomaly").cast("int"))
    .withColumn("severity",
        when(col("anomaly_score") >= 2, "CRITICAL")
        .when(col("anomaly_score") == 1, "WARNING")
        .otherwise("NORMAL"))
    .withColumn("processed_at", current_timestamp())
)

silver_iot.writeStream \\
    .option("checkpointLocation", "/tmp/checkpoint/iot_silver") \\
    .trigger(availableNow=True) \\
    .table("iot.silver_sensors") \\
    .awaitTermination()

print("✅ Silver IoT traitée")`}
              />
              <CodeBlock
                language="python"
                title="Silver — Table des alertes"
                code={`# Table des alertes (WARNING + CRITICAL uniquement)
alerts = (spark.readStream
    .table("iot.silver_sensors")
    .filter("severity IN ('WARNING', 'CRITICAL')")
)

alerts.writeStream \\
    .option("checkpointLocation", "/tmp/checkpoint/iot_alerts") \\
    .trigger(availableNow=True) \\
    .table("iot.silver_alerts") \\
    .awaitTermination()

print("✅ Table des alertes créée")
display(spark.table("iot.silver_alerts").limit(10))`}
              />
            </SolutionToggle>

            <InfoBox type="tip" title="Conseil">
              En production, vous pourriez connecter la table{" "}
              <code>silver_alerts</code> à un système de notification
              (Slack, email, PagerDuty) via un{" "}
              <code>foreachBatch</code> pour alerter les opérateurs en
              temps réel.
            </InfoBox>
          </div>
        </section>

        {/* ====================== ÉTAPE 4 ====================== */}
        <section className="mb-14">
          <div className="flex items-center gap-3 mb-2">
            <span className="w-8 h-8 flex items-center justify-center bg-[#ff3621] text-white text-sm font-bold rounded-full">
              4
            </span>
            <h2 className="text-2xl font-bold text-[#1b3a4b]">
              Couche Gold — Agrégations fenêtrées
            </h2>
          </div>
          <div className="flex items-center gap-3 mb-5 ml-11">
            <span className="text-xs font-medium bg-gray-100 text-gray-600 px-2.5 py-1 rounded-full">
              ⏱ 1h30
            </span>
          </div>

          <div className="ml-11 space-y-4">
            <h3 className="text-lg font-semibold text-[#1b3a4b]">
              📖 Objectif
            </h3>
            <p className="text-gray-700 leading-relaxed">
              Créer des tables Gold avec des agrégations : statistiques par
              capteur et agrégations par fenêtre horaire avec{" "}
              <strong>watermark</strong> pour le streaming.
            </p>

            <InfoBox type="info" title="Watermarks et fenêtres">
              Le{" "}
              <code>withWatermark(&quot;event_time&quot;, &quot;1 hour&quot;)</code>{" "}
              indique à Spark de ne plus attendre les données arrivant avec
              plus d&apos;une heure de retard. Cela permet de libérer la
              mémoire et de produire des résultats fiables en streaming.
            </InfoBox>

            <h3 className="text-lg font-semibold text-[#1b3a4b]">
              📝 Instructions
            </h3>
            <ol className="list-decimal list-inside space-y-2 text-gray-700">
              <li>
                Créez <code>gold_sensor_stats</code> : statistiques agrégées
                par capteur (moyenne, min, max de chaque mesure).
              </li>
              <li>
                Créez <code>gold_hourly_stats</code> : agrégations par
                fenêtre d&apos;une heure et par emplacement.
              </li>
              <li>
                Utilisez <code>outputMode(&quot;complete&quot;)</code> pour
                les statistiques par capteur et{" "}
                <code>outputMode(&quot;append&quot;)</code> pour les
                agrégations horaires.
              </li>
            </ol>

            <SolutionToggle id="sol-iot-etape4">
              <p className="text-sm font-semibold text-gray-600 mb-2">
                Code complet — Couche Gold :
              </p>
              <CodeBlock
                language="python"
                title="Gold 1 : Statistiques par capteur"
                code={`from pyspark.sql.functions import avg, min, max, count, sum, window, when, col

# Gold 1 : Statistiques par capteur
gold_sensor_stats = (spark.readStream
    .table("iot.silver_sensors")
    .groupBy("sensor_id", "location")
    .agg(
        avg("temperature").alias("avg_temp"),
        min("temperature").alias("min_temp"),
        max("temperature").alias("max_temp"),
        avg("pressure").alias("avg_pressure"),
        avg("humidity").alias("avg_humidity"),
        count("*").alias("total_readings"),
        sum(col("anomaly_score").cast("int")).alias("total_anomalies"),
        avg("battery_level").alias("avg_battery")
    )
)

gold_sensor_stats.writeStream \\
    .option("checkpointLocation", "/tmp/checkpoint/gold_sensor_stats") \\
    .outputMode("complete") \\
    .trigger(availableNow=True) \\
    .table("iot.gold_sensor_stats") \\
    .awaitTermination()

print("✅ Gold Sensor Stats créée")`}
              />
              <CodeBlock
                language="python"
                title="Gold 2 : Agrégations par fenêtre horaire"
                code={`# Gold 2 : Alertes par heure (avec watermark)
gold_hourly = (spark.readStream
    .table("iot.silver_sensors")
    .withWatermark("event_time", "1 hour")
    .groupBy(
        window("event_time", "1 hour"),
        "location"
    )
    .agg(
        count("*").alias("readings"),
        sum(when(col("severity") == "CRITICAL", 1)
            .otherwise(0)).alias("critical_count"),
        sum(when(col("severity") == "WARNING", 1)
            .otherwise(0)).alias("warning_count"),
        avg("temperature").alias("avg_temp")
    )
)

gold_hourly.writeStream \\
    .option("checkpointLocation", "/tmp/checkpoint/gold_hourly") \\
    .outputMode("append") \\
    .trigger(availableNow=True) \\
    .table("iot.gold_hourly_stats") \\
    .awaitTermination()

print("✅ Gold Hourly Stats créée")`}
              />
            </SolutionToggle>

            <InfoBox type="warning" title="Output Mode">
              <code>complete</code> renvoie toutes les lignes à chaque
              trigger (adapté aux petites agrégations). <code>append</code>{" "}
              n&apos;écrit que les nouvelles lignes finalisées (nécessite un
              watermark). Ne confondez pas les deux modes !
            </InfoBox>
          </div>
        </section>

        {/* ====================== ÉTAPE 5 ====================== */}
        <section className="mb-14">
          <div className="flex items-center gap-3 mb-2">
            <span className="w-8 h-8 flex items-center justify-center bg-[#ff3621] text-white text-sm font-bold rounded-full">
              5
            </span>
            <h2 className="text-2xl font-bold text-[#1b3a4b]">
              Dashboard &amp; Requêtes d&apos;analyse
            </h2>
          </div>
          <div className="flex items-center gap-3 mb-5 ml-11">
            <span className="text-xs font-medium bg-gray-100 text-gray-600 px-2.5 py-1 rounded-full">
              ⏱ 30 min
            </span>
          </div>

          <div className="ml-11 space-y-4">
            <h3 className="text-lg font-semibold text-[#1b3a4b]">
              📖 Objectif
            </h3>
            <p className="text-gray-700 leading-relaxed">
              Écrire les requêtes d&apos;analyse qui alimenteraient un
              dashboard de monitoring : vue d&apos;ensemble des capteurs,
              alertes récentes et capteurs avec batterie faible.
            </p>

            <InfoBox type="tip" title="Databricks SQL Dashboards">
              Dans Databricks, vous pouvez créer des dashboards SQL
              directement à partir de ces requêtes. Utilisez l&apos;onglet{" "}
              <strong>SQL Editor</strong> puis{" "}
              <strong>New Dashboard</strong> pour créer des visualisations
              interactives.
            </InfoBox>

            <h3 className="text-lg font-semibold text-[#1b3a4b]">
              📝 Instructions
            </h3>
            <ol className="list-decimal list-inside space-y-2 text-gray-700">
              <li>
                Affichez la vue d&apos;ensemble des capteurs triée par
                nombre d&apos;anomalies.
              </li>
              <li>
                Listez les 20 alertes les plus récentes avec les détails
                des mesures.
              </li>
              <li>
                Identifiez les capteurs avec une batterie moyenne inférieure
                à 20%.
              </li>
            </ol>

            <SolutionToggle id="sol-iot-etape5">
              <p className="text-sm font-semibold text-gray-600 mb-2">
                Code complet — Requêtes Dashboard :
              </p>
              <CodeBlock
                language="sql"
                title="Vue d'ensemble des capteurs"
                code={`-- Vue d'ensemble des capteurs (triée par anomalies)
SELECT * FROM iot.gold_sensor_stats
ORDER BY total_anomalies DESC;`}
              />
              <CodeBlock
                language="sql"
                title="Alertes récentes"
                code={`-- Les 20 alertes les plus récentes
SELECT 
  sensor_id, 
  location, 
  severity, 
  temperature, 
  pressure, 
  humidity, 
  event_time
FROM iot.silver_alerts
ORDER BY event_time DESC
LIMIT 20;`}
              />
              <CodeBlock
                language="sql"
                title="Capteurs avec batterie faible"
                code={`-- Capteurs avec batterie faible (< 20%)
SELECT 
  sensor_id, 
  location, 
  avg_battery
FROM iot.gold_sensor_stats
WHERE avg_battery < 20
ORDER BY avg_battery;`}
              />
              <CodeBlock
                language="sql"
                title="Synthèse des anomalies par emplacement"
                code={`-- Bonus : Synthèse par emplacement
SELECT 
  location,
  SUM(total_readings) AS total_mesures,
  SUM(total_anomalies) AS total_anomalies,
  ROUND(SUM(total_anomalies) * 100.0 / SUM(total_readings), 2) 
    AS pct_anomalies
FROM iot.gold_sensor_stats
GROUP BY location
ORDER BY pct_anomalies DESC;`}
              />
            </SolutionToggle>

            <InfoBox type="important" title="Monitoring en production">
              En production, ces requêtes seraient exécutées
              automatiquement via des <strong>Databricks SQL Alerts</strong>{" "}
              pour déclencher des notifications lorsque le taux
              d&apos;anomalies dépasse un seuil défini.
            </InfoBox>
          </div>
        </section>

        {/* Résumé */}
        <section className="mb-14">
          <div className="bg-gradient-to-r from-[#1b3a4b] to-[#2d5f7a] rounded-xl p-6 text-white">
            <h2 className="text-xl font-bold mb-4">
              🎓 Récapitulatif du projet
            </h2>
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
              <div className="bg-white/10 rounded-lg p-4">
                <div className="text-2xl mb-2">📡</div>
                <h3 className="font-bold mb-1">Simulation</h3>
                <p className="text-sm text-white/80">
                  250 mesures de 5 capteurs avec ~10% d&apos;anomalies
                  intégrées.
                </p>
              </div>
              <div className="bg-white/10 rounded-lg p-4">
                <div className="text-2xl mb-2">🥉</div>
                <h3 className="font-bold mb-1">Bronze</h3>
                <p className="text-sm text-white/80">
                  Ingestion Auto Loader avec métadonnées (fichier source,
                  horodatage).
                </p>
              </div>
              <div className="bg-white/10 rounded-lg p-4">
                <div className="text-2xl mb-2">🥈</div>
                <h3 className="font-bold mb-1">Silver</h3>
                <p className="text-sm text-white/80">
                  Détection d&apos;anomalies avec scoring et table
                  d&apos;alertes dédiée.
                </p>
              </div>
              <div className="bg-white/10 rounded-lg p-4">
                <div className="text-2xl mb-2">🥇</div>
                <h3 className="font-bold mb-1">Gold</h3>
                <p className="text-sm text-white/80">
                  Agrégations par capteur et fenêtres horaires avec watermark.
                </p>
              </div>
            </div>
          </div>
        </section>

        {/* Navigation bas de page */}
        <div className="flex flex-wrap gap-4 justify-between items-center pt-8 border-t border-gray-200">
          <Link
            href="/exercices"
            className="inline-flex items-center gap-2 px-5 py-2.5 rounded-lg bg-gray-100 text-[#1b3a4b] font-semibold hover:bg-gray-200 transition-colors"
          >
            ← Tous les exercices
          </Link>
          <Link
            href="/programme"
            className="inline-flex items-center gap-2 px-5 py-2.5 rounded-lg bg-[#ff3621] text-white font-semibold hover:bg-[#e02e1a] transition-colors"
          >
            📅 Voir le programme →
          </Link>
        </div>
      </div>
    </div>
  );
}
