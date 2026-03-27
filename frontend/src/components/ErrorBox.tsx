export function ErrorBox({ message }: { message: string }) {
  return <section className="card error">Ошибка загрузки: {message}</section>;
}
