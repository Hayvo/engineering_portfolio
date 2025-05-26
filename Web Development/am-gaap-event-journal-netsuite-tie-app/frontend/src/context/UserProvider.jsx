import { useState, useEffect } from "react";
import PropTypes from "prop-types";
import UserContext from "./UserContext";

const UserProvider = ({ children }) => {
  const [user, setUserState] = useState(() => {
    const storedUser = localStorage.getItem("user");
    if (storedUser) {
      const { user, expiresAt } = JSON.parse(storedUser);
      if (new Date().getTime() > expiresAt) {
        localStorage.removeItem("user");
        return null;
      }
      return user;
    }
    return null;
  });

  const setUser = (userData) => {
    if (userData) {
      const expiresAt = new Date().getTime() + 60 * 60 * 1000;
      localStorage.setItem(
        "user",
        JSON.stringify({ user: userData, expiresAt })
      );
    } else {
      localStorage.removeItem("user");
    }
    setUserState(userData);
  };

  useEffect(() => {
    const interval = setInterval(() => {
      const storedUser = localStorage.getItem("user");
      if (storedUser) {
        const { expiresAt } = JSON.parse(storedUser);
        if (new Date().getTime() > expiresAt) {
          setUser(null);
        }
      }
    }, 1000 * 60);

    return () => clearInterval(interval);
  }, []);

  return (
    <UserContext.Provider value={{ user, setUser }}>
      {children}
    </UserContext.Provider>
  );
};

UserProvider.propTypes = {
  children: PropTypes.node.isRequired,
};

export default UserProvider;
