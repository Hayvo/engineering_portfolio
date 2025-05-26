import { GoogleOAuthProvider, GoogleLogin } from "@react-oauth/google";
import { useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import { useUser } from "../context/UserContext";

const Login = () => {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const navigate = useNavigate();
  const { user, setUser } = useUser();
  const API_URL = import.meta.env.VITE_API_URL;

  useEffect(() => {
    if (user) {
      // ✅ Auto-login if user context exists
      console.log("User session found. Redirecting to home...");
      navigate("/home");
    }
  }, [user, navigate]); // Runs only when `user` changes

  const login = async (id_token) => {
    setLoading(true);
    try {
      const response = await fetch(`${API_URL}/login`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        credentials: "include",
        body: JSON.stringify({
          id_token: id_token,
        }),
      });

      if (!response.ok) throw new Error("Login failed");

      const data = await response.json();
      console.log("Login Success:", data);
      const user = data.user;
      setUser(user); // ✅ Store user in context

      navigate("/home"); // ✅ Redirect after login
    } catch (error) {
      console.error("Login Error:", error);
      setError("Failed to login. Please try again.");
    } finally {
      setLoading(false);
    }
  };

  const clientId =
    "your-client-id.apps.googleusercontent.com"; // Replace with your actual client ID

  return (
    <GoogleOAuthProvider clientId={clientId}>
      <div className="flex items-center justify-center w-screen h-screen bg-salt">
        <div className="bg-white shadow-lg rounded-2xl p-8 w-96 text-center flex flex-col items-center justify-center gap-6 min-h-[350px]">
          <img
            src="/2022_AM_Logo_Berry.svg"
            alt="AdoreMe Logo"
            className="w-24"
          />
          <h1 className="text-3xl font-clever text-berry">
            Welcome to AdoreMe
          </h1>
          <p className="text-am-black font-montserrat text-sm">
            Sign in to continue
          </p>

          <GoogleLogin
            onSuccess={(credentialResponse) => {
              if (!credentialResponse.credential) {
                setError("No credential received");
                return;
              }
              login(credentialResponse.credential);
            }}
            onError={() => {
              console.log("Login Failed");
              setError("Login failed. Please try again.");
            }}
          />

          {loading && (
            <p className="text-gray-500 font-montserrat">Logging in...</p>
          )}
          {error && <p className="text-red-500 font-montserrat">{error}</p>}
        </div>
      </div>
    </GoogleOAuthProvider>
  );
};

export default Login;
