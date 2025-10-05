import { NavLink, Outlet, Route, Routes } from "react-router-dom";

import Audiences from "./pages/Audiences";
import Console from "./pages/Console";
import Creatives from "./pages/Creatives";
import ExportPage from "./pages/Export";
import Images from "./pages/Images";
import Insights from "./pages/Insights";
import QA from "./pages/QA";
import Wizard from "./pages/Wizard";

const links = [
  { to: "/wizard", label: "Wizard" },
  { to: "/console", label: "Console" },
  { to: "/insights", label: "Insights" },
  { to: "/audiences", label: "Audiences" },
  { to: "/creatives", label: "Creatives" },
  { to: "/images", label: "Images" },
  { to: "/qa", label: "QA" },
  { to: "/export", label: "Export" }
];

function Layout() {
  return (
    <div className="layout">
      <aside className="sidebar">
        <h1>Andronoma</h1>
        <nav>
          {links.map((link) => (
            <NavLink key={link.to} to={link.to} className={({ isActive }) => (isActive ? "active" : "")}> 
              {link.label}
            </NavLink>
          ))}
        </nav>
      </aside>
      <main className="main">
        <Outlet />
      </main>
    </div>
  );
}

export default function App() {
  return (
    <Routes>
      <Route element={<Layout />}>
        <Route path="/" element={<Wizard />} />
        <Route path="/wizard" element={<Wizard />} />
        <Route path="/console" element={<Console />} />
        <Route path="/insights" element={<Insights />} />
        <Route path="/audiences" element={<Audiences />} />
        <Route path="/creatives" element={<Creatives />} />
        <Route path="/images" element={<Images />} />
        <Route path="/qa" element={<QA />} />
        <Route path="/export" element={<ExportPage />} />
      </Route>
    </Routes>
  );
}
