package aldwxstat.aldwls

/**
  * Created by 18510 on 2018/1/10.
  */
trait PublicMysqlInsertData {
    def sceneInsert2db()
    def sceneGroupInsert2db()
    def hourSceneGroupInsert2db()
    def hourSceneInsert2db()
    def trendInsert2db()
    def hourTrendInsert2db()
}
