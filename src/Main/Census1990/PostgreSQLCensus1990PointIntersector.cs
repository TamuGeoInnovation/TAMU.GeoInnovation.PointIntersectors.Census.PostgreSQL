using System;
using System.Data;
using System.Data.SqlClient;
using TAMU.GeoInnovation.PointIntersectors.Census.Census1990;
using USC.GISResearchLab.AddressProcessing.Core.Standardizing.StandardizedAddresses.Lines.LastLines;
using USC.GISResearchLab.Common.Databases.QueryManagers;
using USC.GISResearchLab.Common.Utils.Databases;

namespace TAMU.GeoInnovation.PointIntersectors.Census.PostgreSQL.Census1990
{
    [Serializable]
    public class PostgreSQLCensus1990PointIntersector : AbstractCensus1990PointIntersector
    {

        #region Properties


        #endregion

        public PostgreSQLCensus1990PointIntersector()
            : base()
        { }

        public PostgreSQLCensus1990PointIntersector(double version, IQueryManager blockFilesQueryManager, IQueryManager stateFilesQueryManager, IQueryManager countryFilesQueryManager)
            : base(version, blockFilesQueryManager, stateFilesQueryManager, countryFilesQueryManager)
        { }

        public PostgreSQLCensus1990PointIntersector(Version version, IQueryManager blockFilesQueryManager, IQueryManager stateFilesQueryManager, IQueryManager countryFilesQueryManager)
            : base(version, blockFilesQueryManager, stateFilesQueryManager, countryFilesQueryManager)
        { }


        public override DataTable GetRecordAsDataTable(double longitude, double latitude, string state, string county, double version)
        {
            DataTable ret = null;

            try
            {
                if ((latitude <= 90 && latitude >= -90) && (longitude <= 180 && longitude >= -180))
                {
                    if (StateUtils.isState(state))
                    {

                        string sql = "";
                        sql += " SELECT ";
                        sql += "  st, ";
                        sql += "  co, ";
                        sql += "  tractBase, ";
                        sql += "  tractSuf ";
                        sql += " FROM ";
                        sql += "[" + state + "]";
                        sql += " WITH (INDEX (idx_geog))";
                        sql += " WHERE ";

                        sql += "  Geography::Point(@latitude, @longitude, 4269).STIntersects(shapeGeog) = 1";

                        SqlCommand cmd = new SqlCommand(sql);
                        cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("latitude", SqlDbType.Decimal, latitude));
                        cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("longitude", SqlDbType.Decimal, longitude));

                        IQueryManager qm = StateFilesQueryManager;
                        qm.AddParameters(cmd.Parameters);
                        ret = qm.ExecuteDataTable(CommandType.Text, cmd.CommandText, true);
                    }
                }
            }
            catch (Exception e)
            {
                throw new Exception("Exception occurred GetBlockRecord: " + e.Message, e);
            }

            return ret;
        }

        public override DataTable GetNearestRecordAsDataTable(double longitude, double latitude, string state, double distanceThreshold)
        {
            DataTable ret = null;

            try
            {
                if ((latitude <= 90 && latitude >= -90) && (longitude <= 180 && longitude >= -180))
                {
                    if (StateUtils.isState(state))
                    {


                        string sql = "";
                        //sql += " USE " + QueryManager.Connection.Database + ";" ;
                        sql += " SELECT ";
                        sql += "  TOP 1 ";
                        sql += "  st, ";
                        sql += "  co, ";
                        sql += "  tractBase, ";
                        sql += "  tractSuf, ";
                        sql += "  Geography::Point(@latitude1, @longitude1, 4269).STDistance(shapeGeog) as dist ";
                        sql += " FROM ";
                        //sql += " [CensusTracts1990].[dbo]." + state;
                        sql += "[" + state + "]";

                        sql += " WHERE ";

                        sql += "  Geography::Point(@latitude2, @longitude2, 4269).STDistance(shapeGeog) <= @distanceThreshold ";

                        sql += "  ORDER BY ";
                        sql += "  dist ";

                        SqlCommand cmd = new SqlCommand(sql);
                        cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("latitude1", SqlDbType.Decimal, latitude));
                        cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("longitude1", SqlDbType.Decimal, longitude));
                        cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("latitude2", SqlDbType.Decimal, latitude));
                        cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("longitude2", SqlDbType.Decimal, longitude));
                        cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("distanceThreshold", SqlDbType.Decimal, distanceThreshold));


                        IQueryManager qm = StateFilesQueryManager;
                        qm.AddParameters(cmd.Parameters);
                        ret = qm.ExecuteDataTable(CommandType.Text, cmd.CommandText, true);
                    }
                }
            }
            catch (Exception e)
            {
                throw new Exception("Exception occurred GetNearestRecordAsDataTable: " + e.Message, e);
            }

            return ret;
        }

        // use the 2010 data for getting the state name
        public override string GetStateName(double longitude, double latitude)
        {
            string ret = "";

            try
            {
                if ((latitude <= 90 && latitude >= -90) && (longitude <= 180 && longitude >= -180))
                {

                    string sql = "";
                    sql += " SELECT ";
                    sql += "  stUsPs10 ";
                    sql += " FROM ";
                    sql += "us_state10 ";
                    sql += " WITH (INDEX (idx_geog))";
                    sql += " WHERE ";

                    // first implementation
                    //sql += "  shapeGeog.STIntersects(Geography::STPointFromText('POINT(" + longitude + " " + latitude + ")', 4269)) = 1";

                    // second implementation - attempt to speed it up by checking intersect on the point not the database row
                    //sql += "  Geography::STPointFromText('POINT(" + longitude + " " + latitude + ")', 4269).STIntersects(shapeGeog) = 1";

                    // third implementation - attempt to speed it up using the geography as native point instead, also included the index in the query
                    sql += "  Geography::Point(@latitude, @longitude, 4269).STIntersects(shapeGeog) = 1";

                    SqlCommand cmd = new SqlCommand(sql);
                    cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("latitude", SqlDbType.Decimal, latitude));
                    cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("longitude", SqlDbType.Decimal, longitude));

                    IQueryManager qm = CountryFilesQueryManager;
                    qm.AddParameters(cmd.Parameters);
                    ret = qm.ExecuteScalarString(CommandType.Text, cmd.CommandText, true);
                }
            }
            catch (Exception e)
            {
                throw new Exception("Exception occurred GetStateName: " + e.Message, e);
            }

            return ret;
        }

    }
}