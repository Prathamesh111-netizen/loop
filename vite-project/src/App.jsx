import { useState } from "react";
import Paper from "@mui/material/Paper";
import Stack from "@mui/material/Stack";
import { styled } from "@mui/material/styles";
import Button from "@mui/material/Button";
import axios from "axios";
import TextField from '@mui/material/TextField';

import "./App.css";
const Item = styled(Paper)(({ theme }) => ({
  backgroundColor: theme.palette.mode === "dark" ? "#1A2027" : "#fff",
  ...theme.typography.body2,
  padding: theme.spacing(1),
  textAlign: "center",
  color: theme.palette.text.secondary,
}));

function App() {
  const [data, setData] = useState(null);
  const [result, setResult] = useState(null);
  const [reportId, setReportId] = useState("");
  
  const TriggerReport = () => {
    console.log("Trigger Report");
    axios.get("http://localhost:5000/trigger_report").then((response) => {
      console.log(response);
      setResult(response.data);
    });
  };

  const getReport = () => {
    console.log("Get Report");
    axios.get(`http://localhost:5000/get_report/${reportId}`).then((response) => {
      console.log(response);
      setData(response.data);
    });
  };

  const handleDownload = (data) => {

    fetch(`http://localhost:5000/download/${data}`)
      .then((response) => {
        // Create a Blob from the response data
        return response.blob();
      })
      .then((blob) => {
        // Create a download link
        const downloadUrl = URL.createObjectURL(blob);
        const link = document.createElement("a");
        link.href = downloadUrl;
        link.download = data;
        link.click();

        // Clean up
        URL.revokeObjectURL(downloadUrl);
      })
      .catch((error) => {
        console.error("Error downloading file:", error);
      });
  };

  return (
    <>
      <div className="main">
        <Stack direction="row" spacing={2}>
          <Item sx={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            flexDirection: 'column',
          }}>
            <Button variant="contained" onClick={TriggerReport}>
              Create Report
            </Button>
            {result && <p>Report ID : {result} generated</p>}
          </Item>
          <Item sx={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
          }}>
            <TextField
              id="outlined-basic"
              label="Report ID"
              variant="outlined"
              value = {reportId}
              onChange = {(e) => setReportId(e.target.value)}
            />
            <Button variant="contained" onClick={getReport}>
              Get Report
            </Button>
            {data && (
              <button onClick={() => handleDownload(data)}>
                Download File
              </button>
            )}
          </Item>
        </Stack>
      </div>
    </>
  );
}

export default App;
