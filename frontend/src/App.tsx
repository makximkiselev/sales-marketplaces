import { Navigate, Route, Routes } from "react-router-dom";
import { Shell } from "./components/Shell";
import HomePage from "./app/page";
import CatalogPage from "./app/catalog/page";
import CatalogContentRatingPage from "./app/catalog/content-rating/page";
import CatalogKpisPage from "./app/catalog/kpis/page";
import DataSourcesRedirectPage from "./app/data/sources/page";
import MonitoringRedirectPage from "./app/monitoring/page";
import PricingAttractivenessPage from "./app/pricing/attractiveness/page";
import PricingDecisionPage from "./app/pricing/decision/page";
import PricingFxRatesRedirectPage from "./app/pricing/fx-rates/page";
import PricingLabPage from "./app/pricing/lab/page";
import PricingPricesPage from "./app/pricing/prices/page";
import PricingPromosPage from "./app/pricing/promos/page";
import PricingSettingsRedirectPage from "./app/pricing/settings/page";
import SalesAbcPage from "./app/sales/abc/page";
import SalesBoostPage from "./app/sales/boost/page";
import SalesCoinvestPage from "./app/sales/coinvest/page";
import SalesElasticityPage from "./app/sales/elasticity/page";
import SalesOverviewPage from "./app/sales/overview/page";
import SalesPromosRedirectPage from "./app/sales/promos/page";
import AttractivenessOverviewRedirectPage from "./app/attractiveness/overview/page";
import SettingsFxRatesPage from "./app/settings/fx-rates/page";
import SettingsMonitoringPage from "./app/settings/monitoring/page";
import SettingsPricingPage from "./app/settings/pricing/page";
import SettingsSourcesPage from "./app/settings/sources/page";

export default function App() {
  return (
    <Shell>
      <Routes>
        <Route path="/" element={<HomePage />} />
        <Route path="/catalog" element={<CatalogPage />} />
        <Route path="/catalog/content-rating" element={<CatalogContentRatingPage />} />
        <Route path="/catalog/kpis" element={<CatalogKpisPage />} />
        <Route path="/data/sources" element={<DataSourcesRedirectPage />} />
        <Route path="/monitoring" element={<MonitoringRedirectPage />} />
        <Route path="/pricing/attractiveness" element={<PricingAttractivenessPage />} />
        <Route path="/pricing/decision" element={<PricingDecisionPage />} />
        <Route path="/pricing/fx-rates" element={<PricingFxRatesRedirectPage />} />
        <Route path="/pricing/lab" element={<PricingLabPage />} />
        <Route path="/pricing/prices" element={<PricingPricesPage />} />
        <Route path="/pricing/promos" element={<PricingPromosPage />} />
        <Route path="/pricing/settings" element={<PricingSettingsRedirectPage />} />
        <Route path="/sales/abc" element={<SalesAbcPage />} />
        <Route path="/sales/boost" element={<SalesBoostPage />} />
        <Route path="/sales/coinvest" element={<SalesCoinvestPage />} />
        <Route path="/sales/elasticity" element={<SalesElasticityPage />} />
        <Route path="/sales/overview" element={<SalesOverviewPage />} />
        <Route path="/sales/promos" element={<SalesPromosRedirectPage />} />
        <Route path="/attractiveness/overview" element={<AttractivenessOverviewRedirectPage />} />
        <Route path="/settings/fx-rates" element={<SettingsFxRatesPage />} />
        <Route path="/settings/monitoring" element={<SettingsMonitoringPage />} />
        <Route path="/settings/pricing" element={<SettingsPricingPage />} />
        <Route path="/settings/sources" element={<SettingsSourcesPage />} />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
    </Shell>
  );
}
