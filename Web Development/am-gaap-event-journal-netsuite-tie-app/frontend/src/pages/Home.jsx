import { useState, useEffect, useMemo } from "react";
import { useUser } from "../context/UserContext";
import ApiProvider from "../providers/ApiProvider";
import { useNavigate } from "react-router-dom";
import DatePicker from "react-datepicker";
import "react-datepicker/dist/react-datepicker.css";

const Home = () => {
  const [journals, setJournals] = useState([]); // Example journals
  const [selectedJournal, setSelectedJournal] = useState("");
  const [dateRange, setDateRange] = useState([null, null]);
  const [startDate, endDate] = dateRange;
  const [downloadUrl, setDownloadUrl] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [fileName, setFileName] = useState("");
  const { user } = useUser();
  const navigate = useNavigate();
  // Redirect to login if user is null
  useEffect(() => {
    if (!user) {
      console.log("No valid user session. Redirecting to login...");
      navigate("/login");
    }
  }, [user, navigate]);

  // ✅ Fix: Use useMemo to prevent re-creating ApiProvider on every render
  const apiProvider = useMemo(() => {
    return user ? new ApiProvider({ accessToken: user.accessToken }) : null;
  }, [user]);

  // ✅ Fix: Fetch journals only once when component mounts
  useEffect(() => {
    if (!apiProvider) return; // Prevent call if apiProvider is null

    const fetchJournals = async () => {
      try {
        const journals = await apiProvider.getJournals();
        setJournals(journals);
      } catch (error) {
        console.error("Failed to fetch journals:", error);
      }
    };

    fetchJournals();
  }, [apiProvider]); // ✅ Will only re-run if apiProvider changes

  const formatDate = (date) => {
    if (!date) return "";
    const pad = (num) => String(num).padStart(2, "0"); // Ensure two-digit format
    return `${date.getFullYear()}_${pad(date.getMonth() + 1)}_${pad(
      date.getDate()
    )}`;
  };

  const handleDownload = () => {
    if (downloadUrl) {
      const link = document.createElement("a");
      link.href = downloadUrl;
      link.download = fileName;
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      window.URL.revokeObjectURL(downloadUrl);
      setDownloadUrl(null);
    }
  };

  const getJournalTieOut = async () => {
    if (!selectedJournal || !startDate || !endDate) {
      setError("Please select a journal and a date range");
      setLoading(false);
      return;
    }

    const formattedStartDate = formatDate(startDate);
    const formattedEndDate = formatDate(endDate);

    try {
      setLoading(true);
      setError("");
      setDownloadUrl(null);
      const { url, filename } = await apiProvider.getJournalTieOut(
        selectedJournal,
        formattedStartDate,
        formattedEndDate
      );
      setDownloadUrl(url);
      setFileName(filename);
    } catch (error) {
      console.error("Failed to get journal tie-out:", error);
      setError(error.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="flex flex-col items-center justify-center w-screen min-h-screen bg-salt p-6">
      <div className="bg-white shadow-xl rounded-lg p-8 w-screen max-w-md">
        <h1 className="text-2xl font-bold text-berry text-center mb-4">
          Adore Me - Journal Tie Out
        </h1>

        {/* Journal Selection */}
        <div className="mb-4">
          <label className="block text-sm font-medium text-amBlack">
            Select Journal
          </label>
          <select
            className="mt-1 block w-full p-2 border border-gray-300 rounded-lg"
            value={selectedJournal}
            onChange={(e) => setSelectedJournal(e.target.value)}
          >
            <option value="" disabled>
              Select a journal
            </option>
            {journals.map((journal, index) => (
              <option key={index} value={journal}>
                {journal}
              </option>
            ))}
          </select>
        </div>

        {/* Date Range Selection */}
        <div className="mb-4">
          <label className="block text-sm font-medium text-amBlack">
            Select Date Range
          </label>
          <DatePicker
            selected={startDate}
            onChange={(update) => setDateRange(update)}
            startDate={startDate}
            endDate={endDate}
            dateFormat={"MM/dd/yyyy"}
            selectsRange
            className="mt-1 block w-full p-2 border border-gray-300 rounded-lg"
            placeholderText="Select date range"
          />
        </div>

        {/* Fetch Button */}
        <button
          onClick={getJournalTieOut}
          disabled={!selectedJournal || !startDate || !endDate || loading}
          className="w-full bg-berry text-white py-2 rounded-lg font-bold hover:bg-opacity-80 transition duration-200 disabled:bg-gray-400"
        >
          {loading ? "Fetching..." : "Get Journal"}
        </button>

        {/* Error Message */}
        {error && <p className="text-red-500 mt-2 text-center">{error}</p>}

        {/* Download Button (Only if file is ready) */}
        {downloadUrl && (
          <button
            onClick={handleDownload}
            className="w-full mt-4 bg-ocean text-white py-2 rounded-lg font-bold hover:bg-opacity-80 transition duration-200"
          >
            Download File
          </button>
        )}
      </div>
    </div>
  );
};

export default Home;
