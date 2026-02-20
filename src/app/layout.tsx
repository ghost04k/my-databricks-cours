import type { Metadata } from "next";
import { Inter, Fira_Code } from "next/font/google";
import "./globals.css";
import Navbar from "@/components/Navbar";
import Footer from "@/components/Footer";
import { ProgressProvider } from "@/components/ProgressContext";

const inter = Inter({
  variable: "--font-inter",
  subsets: ["latin"],
});

const firaCode = Fira_Code({
  variable: "--font-fira-code",
  subsets: ["latin"],
});

export const metadata: Metadata = {
  title: "Databricks Cours - Certification Data Engineer Associate",
  description:
    "Guide de formation complet pour la certification Databricks Data Engineer Associate. Apprenez le Lakehouse, Spark SQL, Delta Live Tables et plus encore.",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="fr">
      <body
        className={`${inter.variable} ${firaCode.variable} font-sans antialiased bg-[var(--color-bg)] text-[var(--color-text)]`}
      >
        <ProgressProvider>
          <Navbar />
          <main className="min-h-screen">{children}</main>
          <Footer />
        </ProgressProvider>
      </body>
    </html>
  );
}
