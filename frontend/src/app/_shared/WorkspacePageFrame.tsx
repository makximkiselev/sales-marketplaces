import { ReactNode } from "react";
import layoutStyles from "./AppPageLayout.module.css";

type Props = {
  children: ReactNode;
  className?: string;
  innerClassName?: string;
};

export function WorkspacePageFrame({
  children,
  className = "",
  innerClassName = "",
}: Props) {
  return (
    <section className={`${layoutStyles.pageFrame} ${className}`.trim()}>
      <div className={`${layoutStyles.pageFrameInner} ${innerClassName}`.trim()}>
        {children}
      </div>
    </section>
  );
}
