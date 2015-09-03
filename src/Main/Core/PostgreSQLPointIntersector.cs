using System;
using System.Data;
using System.Data.SqlClient;
using TAMU.GeoInnovation.PointIntersectors.Census.Census2010;
using USC.GISResearchLab.AddressProcessing.Core.Standardizing.StandardizedAddresses.Lines.LastLines;
using USC.GISResearchLab.Common.Databases.QueryManagers;
using USC.GISResearchLab.Common.Utils.Databases;
using TAMU.GeoInnovation.PointIntersectors.Census.PointIntersecters.AbstractClasses;
using TAMU.GeoInnovation.PointIntersectors.Census.OutputData.CensusRecords;

namespace TAMU.GeoInnovation.PointIntersectors.Census.PostgreSQL.Core
{

    [Serializable]
    public class PostgreSQLPointIntersector : AbstractPointIntersector
    {

        #region Properties



        #endregion

        public PostgreSQLPointIntersector()
            : base()
        { }

        public PostgreSQLPointIntersector(IQueryManager referenceDataQueryManager)
            : base(referenceDataQueryManager)
        { }


        public override Object GetRecord(double longitude, double latitude, string table, string shapeField)
        {

            IntersectionRecord ret =  new IntersectionRecord();
            ret.Created = DateTime.Now;
            DateTime start = DateTime.Now;

            ret.InputLatitude = latitude;
            ret.InputLongitude = longitude;

            try
            {
                if ((latitude <= 90 && latitude >= -90) && (longitude <= 180 && longitude >= -180))
                {

                    string sql = "";

                    sql += " SELECT ";
                    sql += "  * ";
                    sql += " FROM ";
                    sql += "" + table + "";
                    sql += " WHERE ";
                    sql += "  ST_Contains(" + shapeField + ", st_geomfromtext('POINT(" + longitude + " " + latitude + ")'))";
                    //sql += "  Contains(" + shapeField + ", GeomFromText(?test)) = 1";

                    SqlCommand cmd = new SqlCommand(sql);

                    //TODO - Come back and try to make this work with a parameterized query - http://stackoverflow.com/questions/8355000/can-i-do-a-parameterized-query-containing-geometry-function
                    //cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("test", SqlDbType.VarChar, "'POINT(" + longitude + " " + latitude + ")'"));

                    IQueryManager qm = ReferenceDataQueryManager;
                   // qm.AddParameters(cmd.Parameters);
                    DataTable dataTable = qm.ExecuteDataTable(CommandType.Text, cmd.CommandText, true);

                    if (dataTable != null && dataTable.Rows.Count > 0)
                    {
                        if (dataTable.Rows.Count == 1)
                        {
                            DataRow dataRow = dataTable.Rows[0];
                            ret = new IntersectionRecord();
                            ret.InputLatitude = latitude;
                            ret.InputLongitude = longitude;
                            ret.MatchedId = Convert.ToString(dataRow["geoId10"]);

                            ret.FieldNames = new string[dataTable.Columns.Count];
                            ret.FieldValues = new object[dataTable.Columns.Count];
                            ret.FieldTypes = new Type[dataTable.Columns.Count];

                            for (int i=0; i< dataTable.Columns.Count; i++)
                            {
                                DataColumn column = dataTable.Columns[i];

                                ret.FieldNames[i] = column.ColumnName;
                                ret.FieldValues[i] = dataRow[column.ColumnName];
                                ret.FieldTypes[i] = column.DataType;
                            }

                        }
                        else
                        {
                            throw new Exception("Returned more than one matching geography - matching count:" + dataTable.Rows.Count);
                        }
                    }

                }
            }
            catch (Exception e)
            {
                ret.Exception = e;
                ret.ExceptionOccurred = true;
                throw new Exception("Exception occurred GetRecord: " + e.Message, e);
            }

            DateTime end = DateTime.Now;
            TimeSpan duration = end.Subtract(start);
            ret.TimeTaken = duration.TotalMilliseconds;
            return ret;
        }

        public override Object GetNearestRecord(double longitude, double latitude, string table, string shapeField, double maxDistance)
        {
        
            DataTable ret = null;

            try
            {
                if ((latitude <= 90 && latitude >= -90) && (longitude <= 180 && longitude >= -180))
                {

                    string sql = "";

                    sql += " SELECT ";
                    sql += "  TOP 1 ";
                    sql += "  stateFp10, ";
                    sql += "  countyFp10, ";
                    sql += "  tractCe10, ";
                    sql += "  blockCe10, ";
                    sql += "  GeoId10, ";

                    sql += "  st_distance(shapeGeom, GeomFromText('POINT(?latitude ?longitude)')) as dist";
                    sql += " FROM ";
                    sql += "'" + table + "'";
                    sql += " WITH (INDEX (idx_geog))";
                    sql += " WHERE ";
                    sql += " st_distance(" + shapeField + ", GeomFromText('POINT(?latitude ?longitude)')) <= ?distanceThreshold ";




                    sql += "  ORDER BY ";
                    sql += "  dist ";

                    SqlCommand cmd = new SqlCommand(sql);
                    cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("latitude1", SqlDbType.Decimal, latitude));
                    cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("longitude1", SqlDbType.Decimal, longitude));
                    cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("latitude2", SqlDbType.Decimal, latitude));
                    cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("longitude2", SqlDbType.Decimal, longitude));
                    cmd.Parameters.Add(SqlParameterUtils.BuildSqlParameter("distanceThreshold", SqlDbType.Decimal, maxDistance));

                    IQueryManager qm = ReferenceDataQueryManager;
                    qm.AddParameters(cmd.Parameters);
                    ret = qm.ExecuteDataTable(CommandType.Text, cmd.CommandText, true);
                }
            }
            catch (Exception e)
            {
                throw new Exception("Exception occurred GetNearestRecord: " + e.Message, e);
            }

            return ret;
        }




    }


}

  