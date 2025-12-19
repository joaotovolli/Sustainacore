import net from "node:net";

const start = Number(process.env.PORT_START || "8001");
const end = Number(process.env.PORT_END || "8050");

const canBind = (port) =>
  new Promise((resolve) => {
    const server = net.createServer();
    server.once("error", () => resolve(false));
    server.once("listening", () => {
      server.close(() => resolve(true));
    });
    server.listen(port, "127.0.0.1");
  });

const run = async () => {
  for (let port = start; port <= end; port += 1) {
    // eslint-disable-next-line no-await-in-loop
    if (await canBind(port)) {
      process.stdout.write(`${port}\n`);
      return;
    }
  }
  process.stderr.write(`No free port found in range ${start}-${end}\n`);
  process.exit(1);
};

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
