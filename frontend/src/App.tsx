import { Navigate, Route, Routes } from "react-router-dom";
import { Shell } from "./components/Shell";
import HomePage from "./app/page";
import CatalogPage from "./app/catalog/CatalogPage";
import CatalogContentRatingPage from "./app/catalog/content-rating/page";
import CatalogKpisPage from "./app/catalog/kpis/page";
import PricingAttractivenessPage from "./app/pricing/attractiveness/AttractivenessPage";
import PricingDecisionPage from "./app/pricing/decision/PricingDecisionPage";
import PricingFxRatesPage from "./app/pricing/fx-rates/PricingFxRatesPage";
import PricingLabPage from "./app/pricing/lab/PricingLabPage";
import PricingPricesPage from "./app/pricing/prices/PricesPage";
import PricingPromosPage from "./app/pricing/promos/PromosPage";
import PricingSettingsPage from "./app/pricing/settings/PricingSettingsPage";
import SalesAbcPage from "./app/sales/abc/SalesAbcPage";
import SalesBoostPage from "./app/sales/boost/SalesBoostPage";
import SalesCoinvestPage from "./app/sales/coinvest/SalesCoinvestPage";
import SalesElasticityPage from "./app/sales/elasticity/SalesElasticityPage";
import SalesOverviewPage from "./app/sales/overview/SalesOverviewPage";
import SettingsMonitoringPage from "./app/settings/monitoring/MonitoringPage";
import SettingsSourcesPage from "./app/settings/sources/DataSourcesPage";

export default function App() {
  return (
    <Shell>
      <Routes>
        <Route path="/" element={<HomePage />} />
        <Route path="/catalog" element={<CatalogPage />} />
        <Route path="/catalog/content-rating" element={<CatalogContentRatingPage />} />
        <Route path="/catalog/kpis" element={<CatalogKpisPage />} />
        <Route path="/data/sources" element={<Navigate to="/settings/sources" replace />} />
        <Route path="/monitoring" element={<Navigate to="/settings/monitoring" replace />} />
        <Route path="/pricing/attractiveness" element={<PricingAttractivenessPage />} />
        <Route path="/pricing/decision" element={<PricingDecisionPage />} />
        <Route path="/pricing/fx-rates" element={<Navigate to="/settings/fx-rates" replace />} />
        <Route path="/pricing/lab" element={<PricingLabPage />} />
        <Route path="/pricing/prices" element={<PricingPricesPage />} />
        <Route path="/pricing/promos" element={<PricingPromosPage />} />
        <Route path="/pricing/settings" element={<Navigate to="/settings/pricing" replace />} />
        <Route path="/sales/abc" element={<SalesAbcPage />} />
        <Route path="/sales/boost" element={<SalesBoostPage />} />
        <Route path="/sales/coinvest" element={<SalesCoinvestPage />} />
        <Route path="/sales/elasticity" element={<SalesElasticityPage />} />
        <Route path="/sales/overview" element={<SalesOverviewPage />} />
        <Route path="/sales/promos" element={<Navigate to="/pricing/promos" replace />} />
        <Route path="/attractiveness/overview" element={<Navigate to="/pricing/attractiveness" replace />} />
        <Route path="/settings/fx-rates" element={<PricingFxRatesPage />} />
        <Route path="/settings/monitoring" element={<SettingsMonitoringPage />} />
        <Route path="/settings/pricing" element={<PricingSettingsPage />} />
        <Route path="/settings/sources" element={<SettingsSourcesPage />} />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
    </Shell>
  );
}
