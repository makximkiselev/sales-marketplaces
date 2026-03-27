import { Navigate } from "react-router-dom";

export default function LegacyMonitoringRedirect() {
  return <Navigate to="/settings/monitoring" replace />;
}
