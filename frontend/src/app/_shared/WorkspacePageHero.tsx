import { ReactNode } from "react";
import { WorkspaceHeader, WorkspaceSurface, WorkspaceTabs, WorkspaceToolbar } from "../../components/page/WorkspaceKit";
import layoutStyles from "./AppPageLayout.module.css";

type HeroTabItem<T extends string> = {
  id: T;
  label: ReactNode;
  meta?: ReactNode;
};

type Props<T extends string> = {
  title: ReactNode;
  subtitle?: ReactNode;
  meta?: ReactNode;
  tabs?: {
    items: Array<HeroTabItem<T>>;
    activeId: T;
    onChange: (id: T) => void;
    className?: string;
  };
  toolbar?: ReactNode;
  toolbarClassName?: string;
  className?: string;
  children?: ReactNode;
};

export function WorkspacePageHero<T extends string>({
  title,
  subtitle,
  meta,
  tabs,
  toolbar,
  toolbarClassName = "",
  className = "",
  children,
}: Props<T>) {
  return (
    <WorkspaceSurface className={`${layoutStyles.heroSurface} ${className}`.trim()}>
      {tabs ? (
        <div className={layoutStyles.heroRail}>
          <WorkspaceTabs
            className={`${layoutStyles.pageTabs} ${tabs.className || ""}`.trim()}
            items={tabs.items}
            activeId={tabs.activeId}
            onChange={tabs.onChange}
          />
        </div>
      ) : null}

      <WorkspaceHeader title={title} subtitle={subtitle} meta={meta} />

      {toolbar ? (
        <WorkspaceToolbar className={`${layoutStyles.toolbar} ${toolbarClassName}`.trim()}>
          {toolbar}
        </WorkspaceToolbar>
      ) : null}

      {children}
    </WorkspaceSurface>
  );
}
