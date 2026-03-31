import { useEffect, useMemo, useState } from "react";
import { ModalShell, PageFrame, PageSectionTitle } from "../../../components/page/PageKit";
import { SectionBlock } from "../../../components/page/SectionKit";
import { KpiCard, KpiGrid } from "../../../components/page/DataKit";
import { WorkspaceHeader, WorkspaceSurface } from "../../../components/page/WorkspaceKit";
import { apiGetOk, apiPostOk } from "../../../lib/api";
import styles from "./AdminPage.module.css";
import layoutStyles from "../../_shared/AppPageLayout.module.css";

type UserRole = "owner" | "manager" | "viewer";

type AdminUser = {
  user_id: string;
  identifier: string;
  display_name: string;
  role: UserRole;
  is_active: boolean;
  created_at?: string;
  updated_at?: string;
};

type UserFormState = {
  identifier: string;
  display_name: string;
  role: UserRole;
  is_active: boolean;
  password: string;
};

type FilterState = {
  role: "all" | UserRole;
  status: "all" | "active" | "disabled";
  search: string;
};

const ROLE_OPTIONS: Array<{ value: UserRole; label: string }> = [
  { value: "owner", label: "Owner" },
  { value: "manager", label: "Manager" },
  { value: "viewer", label: "Viewer" },
];

function generatePassword(length = 14) {
  const alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz23456789!@#$%";
  const array = new Uint32Array(length);
  crypto.getRandomValues(array);
  return Array.from(array, (value) => alphabet[value % alphabet.length]).join("");
}

function emptyForm(): UserFormState {
  return {
    identifier: "",
    display_name: "",
    role: "viewer",
    is_active: true,
    password: "",
  };
}

export default function SettingsAdminPage() {
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");
  const [users, setUsers] = useState<AdminUser[]>([]);
  const [form, setForm] = useState<UserFormState>(() => ({ ...emptyForm(), password: generatePassword() }));
  const [editUser, setEditUser] = useState<AdminUser | null>(null);
  const [editForm, setEditForm] = useState<UserFormState | null>(null);
  const [confirmUser, setConfirmUser] = useState<AdminUser | null>(null);
  const [filters, setFilters] = useState<FilterState>({ role: "all", status: "all", search: "" });

  async function loadUsers() {
    setLoading(true);
    setError("");
    try {
      const data = await apiGetOk<{ ok: boolean; rows: AdminUser[] }>("/api/admin/users");
      setUsers(Array.isArray(data.rows) ? data.rows : []);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    void loadUsers();
  }, []);

  const ownersCount = useMemo(() => users.filter((user) => user.role === "owner" && user.is_active).length, [users]);
  const activeUsersCount = useMemo(() => users.filter((user) => user.is_active).length, [users]);
  const filteredUsers = useMemo(() => {
    const query = filters.search.trim().toLowerCase();
    return users.filter((user) => {
      if (filters.role !== "all" && user.role !== filters.role) return false;
      if (filters.status === "active" && !user.is_active) return false;
      if (filters.status === "disabled" && user.is_active) return false;
      if (!query) return true;
      return `${user.identifier} ${user.display_name} ${user.role}`.toLowerCase().includes(query);
    });
  }, [filters, users]);

  async function handleCreate() {
    setSaving(true);
    setError("");
    try {
      await apiPostOk("/api/admin/users", {
        identifier: form.identifier,
        display_name: form.display_name,
        role: form.role,
        is_active: form.is_active,
        password: form.password,
      });
      setForm({ ...emptyForm(), password: generatePassword() });
      await loadUsers();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSaving(false);
    }
  }

  async function copyPassword(value: string) {
    if (!value.trim()) return;
    try {
      await navigator.clipboard.writeText(value);
    } catch {
      setError("Не удалось скопировать пароль");
    }
  }

  function startEdit(user: AdminUser) {
    setEditUser(user);
    setEditForm({
      identifier: user.identifier,
      display_name: user.display_name,
      role: user.role,
      is_active: user.is_active,
      password: "",
    });
  }

  async function handleUpdate() {
    if (!editUser || !editForm) return;
    setSaving(true);
    setError("");
    try {
      await apiPostOk(`/api/admin/users/${editUser.user_id}`, {
        display_name: editForm.display_name,
        role: editForm.role,
        is_active: editForm.is_active,
        password: editForm.password || undefined,
      });
      setEditUser(null);
      setEditForm(null);
      await loadUsers();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSaving(false);
    }
  }

  async function handleConfirmDisable() {
    if (!confirmUser) return;
    setSaving(true);
    setError("");
    try {
      await apiPostOk(`/api/admin/users/${confirmUser.user_id}`, { is_active: false });
      setConfirmUser(null);
      await loadUsers();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSaving(false);
    }
  }

  return (
    <PageFrame
      title="Администрирование"
      subtitle="Управление доступом пользователей, ролями и паролями для панели."
      meta={ownersCount ? `Активных владельцев: ${ownersCount}` : undefined}
    >
      <div className={layoutStyles.shell}>
        <WorkspaceSurface className={layoutStyles.heroSurface}>
          <WorkspaceHeader
            title="Access control"
            subtitle="Центр управления пользователями панели, ролями доступа и ручной выдачей паролей."
            meta={(
              <div className={layoutStyles.heroMeta}>
                <span className={layoutStyles.metaChip}>Активных: {activeUsersCount}</span>
                <span className={layoutStyles.metaChip}>Owners: {ownersCount}</span>
              </div>
            )}
          />
          <KpiGrid>
            <KpiCard label="Пользователи" value={users.length.toLocaleString("ru-RU")} />
            <KpiCard label="Активные" value={activeUsersCount.toLocaleString("ru-RU")} />
            <KpiCard label="По фильтру" value={filteredUsers.length.toLocaleString("ru-RU")} />
          </KpiGrid>
        </WorkspaceSurface>

        <SectionBlock>
          <div className={styles.layout}>
            <div className={styles.createCard}>
              <PageSectionTitle title="Новый пользователь" />
              <div className={styles.formGrid}>
                <label className={styles.field}>
                  <span className={styles.label}>Логин</span>
                  <input
                    className="input input-size-lg"
                    value={form.identifier}
                    onChange={(e) => setForm((prev) => ({ ...prev, identifier: e.target.value }))}
                    placeholder="например, manager"
                  />
                </label>
                <label className={styles.field}>
                  <span className={styles.label}>Имя</span>
                  <input
                    className="input input-size-lg"
                    value={form.display_name}
                    onChange={(e) => setForm((prev) => ({ ...prev, display_name: e.target.value }))}
                    placeholder="Имя в интерфейсе"
                  />
                </label>
                <label className={styles.field}>
                  <span className={styles.label}>Роль</span>
                  <select
                    className="input input-size-md"
                    value={form.role}
                    onChange={(e) => setForm((prev) => ({ ...prev, role: e.target.value as UserRole }))}
                  >
                    {ROLE_OPTIONS.map((option) => (
                      <option key={option.value} value={option.value}>{option.label}</option>
                    ))}
                  </select>
                </label>
                <label className={styles.field}>
                  <span className={styles.label}>Статус</span>
                  <select
                    className="input input-size-md"
                    value={form.is_active ? "active" : "disabled"}
                    onChange={(e) => setForm((prev) => ({ ...prev, is_active: e.target.value === "active" }))}
                  >
                    <option value="active">Активен</option>
                    <option value="disabled">Отключен</option>
                  </select>
                </label>
                <label className={`${styles.field} ${styles.fieldWide}`}>
                  <span className={styles.label}>Пароль</span>
                  <div className={styles.passwordRow}>
                    <input
                      className="input input-size-fluid"
                      value={form.password}
                      onChange={(e) => setForm((prev) => ({ ...prev, password: e.target.value }))}
                      placeholder="Введите пароль"
                    />
                    <button type="button" className="btn ghost" onClick={() => setForm((prev) => ({ ...prev, password: generatePassword() }))}>
                      Сгенерировать
                    </button>
                    <button type="button" className="btn ghost" onClick={() => void copyPassword(form.password)}>
                      Скопировать
                    </button>
                  </div>
                </label>
              </div>
              <div className={styles.actions}>
                <button
                  type="button"
                  className="btn primary"
                  disabled={saving || !form.identifier.trim() || !form.password.trim()}
                  onClick={() => void handleCreate()}
                >
                  Создать пользователя
                </button>
              </div>
            </div>

            <div className={styles.listCard}>
              <PageSectionTitle title="Пользователи" meta={loading ? "Загрузка..." : `Всего: ${filteredUsers.length}`} />
              {error ? <div className="status error">{error}</div> : null}
              <div className={styles.filtersBar}>
                <label className={styles.filterField}>
                  <span className={styles.label}>Поиск</span>
                  <input
                    className="input input-size-fluid"
                    value={filters.search}
                    onChange={(e) => setFilters((prev) => ({ ...prev, search: e.target.value }))}
                    placeholder="Логин или имя"
                  />
                </label>
                <label className={styles.filterField}>
                  <span className={styles.label}>Роль</span>
                  <select
                    className="input input-size-md"
                    value={filters.role}
                    onChange={(e) => setFilters((prev) => ({ ...prev, role: e.target.value as FilterState["role"] }))}
                  >
                    <option value="all">Все роли</option>
                    {ROLE_OPTIONS.map((option) => (
                      <option key={option.value} value={option.value}>{option.label}</option>
                    ))}
                  </select>
                </label>
                <label className={styles.filterField}>
                  <span className={styles.label}>Статус</span>
                  <select
                    className="input input-size-md"
                    value={filters.status}
                    onChange={(e) => setFilters((prev) => ({ ...prev, status: e.target.value as FilterState["status"] }))}
                  >
                    <option value="all">Все</option>
                    <option value="active">Активные</option>
                    <option value="disabled">Отключенные</option>
                  </select>
                </label>
              </div>
              <div className={styles.userList}>
                {filteredUsers.map((user) => (
                  <article key={user.user_id} className={styles.userCard}>
                    <div className={styles.userHead}>
                      <div className={styles.userMain}>
                        <div className={styles.userName}>{user.display_name || user.identifier}</div>
                        <div className={styles.userLogin}>@{user.identifier}</div>
                      </div>
                      <div className={styles.userBadges}>
                        <span className={styles.roleBadge}>{user.role}</span>
                        <span className={user.is_active ? styles.activeBadge : styles.disabledBadge}>
                          {user.is_active ? "Активен" : "Отключен"}
                        </span>
                      </div>
                    </div>
                    <div className={styles.userMeta}>
                      <span>Создан: {user.created_at ? new Date(user.created_at).toLocaleString("ru-RU") : "—"}</span>
                      <span>Обновлен: {user.updated_at ? new Date(user.updated_at).toLocaleString("ru-RU") : "—"}</span>
                    </div>
                    <div className={styles.userActions}>
                      <button type="button" className="btn ghost" onClick={() => startEdit(user)}>Редактировать</button>
                      {user.is_active ? (
                        <button type="button" className="btn danger" onClick={() => setConfirmUser(user)}>
                          Отключить
                        </button>
                      ) : null}
                    </div>
                  </article>
                ))}
                {!loading && !filteredUsers.length ? <div className={styles.emptyState}>Пользователи по текущему фильтру не найдены.</div> : null}
              </div>
            </div>
          </div>
        </SectionBlock>
      </div>

      {editUser && editForm ? (
        <ModalShell
          title="Редактирование пользователя"
          subtitle={editUser.identifier}
          onClose={() => {
            setEditUser(null);
            setEditForm(null);
          }}
          width="min(92vw, 720px)"
        >
          <div className={styles.formGrid}>
            <label className={styles.field}>
              <span className={styles.label}>Логин</span>
              <input className="input input-size-lg" value={editForm.identifier} disabled />
            </label>
            <label className={styles.field}>
              <span className={styles.label}>Имя</span>
              <input
                className="input input-size-lg"
                value={editForm.display_name}
                onChange={(e) => setEditForm((prev) => (prev ? { ...prev, display_name: e.target.value } : prev))}
              />
            </label>
            <label className={styles.field}>
              <span className={styles.label}>Роль</span>
              <select
                className="input input-size-md"
                value={editForm.role}
                onChange={(e) => setEditForm((prev) => (prev ? { ...prev, role: e.target.value as UserRole } : prev))}
              >
                {ROLE_OPTIONS.map((option) => (
                  <option key={option.value} value={option.value}>{option.label}</option>
                ))}
              </select>
            </label>
            <label className={styles.field}>
              <span className={styles.label}>Статус</span>
              <select
                className="input input-size-md"
                value={editForm.is_active ? "active" : "disabled"}
                onChange={(e) => setEditForm((prev) => (prev ? { ...prev, is_active: e.target.value === "active" } : prev))}
              >
                <option value="active">Активен</option>
                <option value="disabled">Отключен</option>
              </select>
            </label>
            <label className={`${styles.field} ${styles.fieldWide}`}>
              <span className={styles.label}>Новый пароль</span>
              <div className={styles.passwordRow}>
                <input
                  className="input input-size-fluid"
                  value={editForm.password}
                  onChange={(e) => setEditForm((prev) => (prev ? { ...prev, password: e.target.value } : prev))}
                  placeholder="Оставь пустым, если не меняешь"
                />
                <button
                  type="button"
                  className="btn ghost"
                  onClick={() => setEditForm((prev) => (prev ? { ...prev, password: generatePassword() } : prev))}
                >
                  Сгенерировать
                </button>
                <button
                  type="button"
                  className="btn ghost"
                  onClick={() => void copyPassword(editForm.password)}
                  disabled={!editForm.password.trim()}
                >
                  Скопировать
                </button>
              </div>
            </label>
          </div>
          <div className={styles.modalActions}>
            <button type="button" className="btn ghost" onClick={() => { setEditUser(null); setEditForm(null); }}>
              Отмена
            </button>
            <button type="button" className="btn primary" disabled={saving} onClick={() => void handleUpdate()}>
              Сохранить
            </button>
          </div>
        </ModalShell>
      ) : null}

      {confirmUser ? (
        <ModalShell
          title="Отключить пользователя"
          subtitle={confirmUser.identifier}
          onClose={() => setConfirmUser(null)}
          width="min(92vw, 520px)"
        >
          <div className={styles.confirmText}>
            Пользователь потеряет доступ к панели сразу после сохранения. Если понадобится, его можно будет включить обратно через редактирование.
          </div>
          <div className={styles.modalActions}>
            <button type="button" className="btn ghost" onClick={() => setConfirmUser(null)}>
              Отмена
            </button>
            <button type="button" className="btn danger" disabled={saving} onClick={() => void handleConfirmDisable()}>
              Отключить
            </button>
          </div>
        </ModalShell>
      ) : null}
    </PageFrame>
  );
}
