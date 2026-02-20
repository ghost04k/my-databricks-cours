"use client";

import ProgressDashboard from "@/components/ProgressDashboard";

export default function HomeProgressSection() {
  return (
    <section className="py-16 px-6 bg-white border-b border-gray-100">
      <div className="max-w-4xl mx-auto">
        <ProgressDashboard />
      </div>
    </section>
  );
}
