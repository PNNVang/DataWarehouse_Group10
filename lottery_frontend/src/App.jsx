import logo from './logo.svg';
import { BrowserRouter, Route, Routes } from 'react-router-dom';
import SideBarComponent from './components/sidebar/SideBarComponent';
import DashboardPage from './components/page/DashBoardPage';
import '../src/css/styles.css'
import HeaderComponent from './components/header/HeaderComponent';
function App() {
  return (
    <div className="App">
      <BrowserRouter>
        <SideBarComponent />
        <div className='main-content'>
          <div className='container'>

            <HeaderComponent />
            <Routes>
              <Route path='/dashboard' element={<DashboardPage />}></Route>
            </Routes>


          </div>
        </div>




      </BrowserRouter>


    </div>
  );
}

export default App;
