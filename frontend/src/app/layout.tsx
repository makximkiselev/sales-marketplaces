import { ReactNode } from "react";
import "../styles/globals.css";
import "../components/ui/primitives.css";
import { Shell } from "../components/Shell";

export const metadata = {
  title: "Аналитика данных Web",
  description: "Фронтенд аналитической платформы"
};

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="ru">
      <body>
        <Shell>{children}</Shell>
      </body>
    </html>
  );
}
