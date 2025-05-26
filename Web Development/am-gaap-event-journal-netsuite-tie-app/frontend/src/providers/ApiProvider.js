class ApiProvider {
  constructor({ accessToken }) {
    this.accessToken = accessToken;
    this.baseUrl = import.meta.env.VITE_API_URL;
  }

  async getJournals() {
    const response = await fetch(`${this.baseUrl}/journals`, {
      method: "GET",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${this.accessToken}`,
      },
      // credentials: "include",
    });

    if (!response.ok) throw new Error("Failed to fetch journals");

    const responseJson = await response.json();
    // console.log("Parsed JSON:", responseJson);

    return responseJson["journals"];
  }

  async getJournalTieOut(journal, dateFrom, dateTo) {
    const response = await fetch(
      `${this.baseUrl}/api?journal=${journal}&dateFrom=${dateFrom}&dateTo=${dateTo}`,
      {
        method: "GET",
        headers: {
          "Content-Type": "application/json",
          "Authorization": `Bearer ${this.accessToken}`,
        },
        // credentials: "include",
      }
    );

    const contentDisposition = response.headers.get("Content-Disposition");
    // console.log("Content-Disposition:", contentDisposition);
    let filename = `journal_${dateFrom}_${dateTo}.xlsx`; // Default

    if (contentDisposition) {
      const match = contentDisposition.match(/filename="([^"]+\.xlsx)"/); // Ensure it ends in .xlsm
      if (match && match[1]) {
        filename = match[1];
      }
    }

    if (!response.ok) throw new Error("Failed to fetch journals");

    const blob = await response.blob();
    const url = window.URL.createObjectURL(blob);

    return { url, filename };
  }

  async deleteFile(fileName) {
    const response = await fetch(`${this.baseUrl}/delete_file`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${this.accessToken}`, // ✅ Send token in headers
      },
      body: JSON.stringify({ fileName: fileName }),
    });

    if (!response.ok) throw new Error("Failed to delete file");

    return true;
  }
}
export default ApiProvider;
